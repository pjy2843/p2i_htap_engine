#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
개선된 배치:
- 동일한 Postgres snapshot에서 여러 테이블을 읽기 위해 단일 REPEATABLE READ 트랜잭션 사용
- 배치 idempotency: batch_id 기반으로 migration_history 검사
- Iceberg append 후 migration_history 기록(가능한 경우 snapshot id 캡처)
- Postgres 반영(삭제/마크)은 Iceberg 성공 후 별도의 Postgres 트랜잭션에서 안전하게 수행
"""
import os, time, uuid, psycopg2, psycopg2.extras
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# 환경설정 (기존 값 유지/환경에 맞게 변경)
PG_USER, PG_PASSWORD = "postgres", "postgres"
PG_HOST, PG_PORT     = "localhost", "5432"
PG_DB                = "chbench"
DSN = f"dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} host={PG_HOST} port={PG_PORT}"

CATALOG = "rest"
NS = "iceberg"
WAREHOUSE = "s3a://vldb-000/iceberg/"

# Spark 세팅 (기존 환경 설정을 유지하세요)
spark = SparkSession.builder \
    .appName("migration-improved-idempotent") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NS}")

def get_conn():
    return psycopg2.connect(DSN)

def batch_already_processed(conn, batch_id, relname):
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM migration_history WHERE batch_id=%s AND relname=%s", (batch_id, relname))
        r = cur.fetchone()
        return bool(r and r[0] == 'success')

def read_snapshot_rows(queries_by_table):
    """
    Read each query within a single REPEATABLE READ transaction to get a consistent snapshot.
    Returns dict relname -> {cols, rows}
    """
    conn = get_conn()
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ)
    cur = conn.cursor()
    results = {}
    try:
        cur.execute("BEGIN;")
        for tbl, sql in queries_by_table.items():
            cur.execute(sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            results[tbl] = {"cols": cols, "rows": rows}
        cur.execute("COMMIT;")
    finally:
        cur.close()
        conn.close()
    return results

def df_from_rows(cols, rows):
    if not rows:
        schema = StructType([StructField(name, StringType(), True) for name in cols])
        return spark.createDataFrame([], schema=schema)
    return spark.createDataFrame(rows, schema=cols)

def append_iceberg_and_get_snapshot(dest, df):
    # Append to Iceberg and return snapshot id if available; fallback to generated id.
    df.coalesce(1).writeTo(dest).append()
    try:
        # If your Iceberg catalog exposes snapshot info via SQL, adjust accordingly.
        res = spark.sql(f"SELECT * FROM {CATALOG}.system.snapshots WHERE namespace = '{NS}' AND table_name = '{dest.split('.')[-1]}'").collect()
        if res:
            return str(res[-1].snapshot_id)
    except Exception:
        pass
    return f"snapshot-{uuid.uuid4()}"

def run_batch(delay_minutes=1):
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=delay_minutes)
    cutoff_iso = cutoff.isoformat()
    pol_conn = get_conn()
    try:
        pol_cur = pol_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        pol_cur.execute("SELECT * FROM sync_policy")
        tasks = pol_cur.fetchall()
    finally:
        pol_conn.close()

    # build per-table queries (adjust as needed)
    queries = {}
    for task in tasks:
        tbl = task['relname']
        if tbl == "order_line":
            sql = f"SELECT * FROM order_line WHERE ol_delivery_d IS NOT NULL AND (ol_delivery_d AT TIME ZONE 'Asia/Seoul') > '{(cutoff - timedelta(minutes=5)).isoformat()}' AND (ol_delivery_d AT TIME ZONE 'Asia/Seoul') <= '{cutoff_iso}'"
        elif tbl == "orders":
            sql = f"""
              SELECT o.* FROM orders o
              JOIN (
                SELECT ol_w_id AS o_w_id, ol_d_id AS o_d_id, ol_o_id AS o_id,
                       MAX(ol_delivery_d AT TIME ZONE 'Asia/Seoul') AS closed_at
                FROM order_line WHERE ol_delivery_d IS NOT NULL
                GROUP BY 1,2,3
              ) x USING (o_w_id, o_d_id, o_id)
              WHERE x.closed_at > '{(cutoff - timedelta(minutes=5)).isoformat()}' AND x.closed_at <= '{cutoff_iso}'
            """
        elif tbl == "hhistory":
            sql = f"SELECT * FROM hhistory WHERE (h_date AT TIME ZONE 'Asia/Seoul') > '{(cutoff - timedelta(minutes=5)).isoformat()}' AND (h_date AT TIME ZONE 'Asia/Seoul') <= '{cutoff_iso}'"
        else:
            continue
        queries[tbl] = sql

    snap_results = read_snapshot_rows(queries)
    for task in tasks:
        tbl = task['relname']
        if tbl not in snap_results: continue
        rows_info = snap_results[tbl]
        if not rows_info['rows']: 
            continue
        batch_id = f"{tbl}:{cutoff_iso}"
        conn = get_conn()
        try:
            if batch_already_processed(conn, batch_id, tbl):
                print(f"[{tbl}] batch {batch_id} already processed -> skip")
                conn.close()
                continue
            with conn.cursor() as cur:
                cur.execute("INSERT INTO migration_history (batch_id, relname, cutoff_ts, status, started_at) VALUES (%s,%s,%s,%s,now()) ON CONFLICT (batch_id, relname) DO UPDATE SET status='processing', started_at=now()", (batch_id, tbl, cutoff, 'processing'))
                conn.commit()
        finally:
            conn.close()

        df = df_from_rows(rows_info['cols'], rows_info['rows'])
        dest = f"{CATALOG}.{NS}.{tbl}"
        print(f"[{tbl}] appending {df.count()} rows to {dest}")
        try:
            snapshot_id = append_iceberg_and_get_snapshot(dest, df)
        except Exception as e:
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute("UPDATE migration_history SET status='failed', notes=%s, finished_at=now() WHERE batch_id=%s AND relname=%s", (str(e), batch_id, tbl))
                conn.commit()
            conn.close()
            raise

        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE migration_history SET status=%s, iceberg_snapshot_id=%s, finished_at=now() WHERE batch_id=%s AND relname=%s", ('success', snapshot_id, batch_id, tbl))
                pk_cols = task['pk_columns']
                cols_def = ", ".join(f"{c} text" for c in pk_cols)
                cur.execute(f"CREATE TEMP TABLE stg_pk_{tbl} ({cols_def}) ON COMMIT DROP;")
                pk_rows = []
                for row in rows_info['rows']:
                    rmap = dict(zip(rows_info['cols'], row))
                    pk_rows.append(tuple(str(rmap[c]) for c in pk_cols))
                psycopg2.extras.execute_values(cur, f"INSERT INTO stg_pk_{tbl} ({', '.join(pk_cols)}) VALUES %s", pk_rows, page_size=1000)
                cond = " AND ".join([f"{tbl}.{c}::text = s.{c}" for c in pk_cols])
                cur.execute(f"DELETE FROM {tbl} USING stg_pk_{tbl} s WHERE {cond};")
                cur.execute("INSERT INTO sync_watermark (relname, last_ts) VALUES (%s,%s) ON CONFLICT(relname) DO UPDATE SET last_ts = EXCLUDED.last_ts", (tbl, cutoff))
                conn.commit()
            print(f"[{tbl}] migration complete; snapshot={snapshot_id}")
        except Exception as e:
            conn.rollback()
            with get_conn().cursor() as c2:
                c2.execute("UPDATE migration_history SET status='failed', notes=%s, finished_at=now() WHERE batch_id=%s AND relname=%s", (str(e), batch_id, tbl))
                c2.connection.commit()
            raise
        finally:
            conn.close()

if __name__ == "__main__":
    run_batch(delay_minutes=1)