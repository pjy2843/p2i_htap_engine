#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
migration_improved.py
- 기존 migration.py 개선판
- 1) snapshot-consistent 읽기 (REPEATABLE READ)
- 2) batch idempotency
- 3) Iceberg append + snapshot id 추출(가능하면)
- 4) Postgres 반영 단계에서 임시 테이블을 원본 테이블의 컬럼 타입으로 생성(타입 안전성)
"""
import os, time, uuid, psycopg2, psycopg2.extras
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# 환경설정(환경에 맞게 변경)
PG_USER, PG_PASSWORD = "postgres", "postgres"
PG_HOST, PG_PORT     = "localhost", "5432"
PG_DB                = "chbench"
DSN = f"dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} host={PG_HOST} port={PG_PORT}"

CATALOG = "rest"
NS = "iceberg"
WAREHOUSE = "s3a://vldb-000/iceberg/"

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
    """같은 REPEATABLE READ 트랜잭션에서 여러 테이블을 읽어 동일 snapshot 보장"""
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
    """데이터를 Iceberg에 append하고 가능한 경우 snapshot id를 반환"""
    df.coalesce(1).writeTo(dest).append()
    try:
        # 카탈로그/환경에 따라 다름: 적절한 방법으로 snapshot id를 가져오세요.
        res = spark.sql(f"SELECT * FROM {CATALOG}.system.snapshots WHERE namespace = '{NS}' AND table_name = '{dest.split('.')[-1]}'").collect()
        if res:
            return str(res[-1].snapshot_id)
    except Exception:
        pass
    return f"snapshot-{uuid.uuid4()}"

def create_temp_stg_like_table(cur, tbl, pk_cols):
    """
    원본 tbl의 타입을 그대로 복사하는 임시 테이블 생성:
    CREATE TEMP TABLE stg_pk_tbl (LIKE tbl INCLUDING DEFAULTS) ON COMMIT DROP;
    그리고 non-pk 컬럼을 제거하여 pk 컬럼만 남김
    """
    stg = f"stg_pk_{tbl}"
    cur.execute(f"CREATE TEMP TABLE {stg} (LIKE {tbl} INCLUDING DEFAULTS) ON COMMIT DROP;")
    # 컬럼 목록 조회
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (tbl,))
    all_cols = [r[0] for r in cur.fetchall()]
    non_pk_cols = [c for c in all_cols if c not in pk_cols]
    for c in non_pk_cols:
        cur.execute(f"ALTER TABLE {stg} DROP COLUMN {c};")
    return stg

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

    # 쿼리 빌드(환경에 맞게 조정)
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
                cur.execute(
                    "INSERT INTO migration_history (batch_id, relname, cutoff_ts, status, started_at) VALUES (%s,%s,%s,%s,now()) "
                    "ON CONFLICT (batch_id, relname) DO UPDATE SET status='processing', started_at=now()",
                    (batch_id, tbl, cutoff, 'processing')
                )
                conn.commit()
        finally:
            conn.close()

        df = df_from_rows(rows_info['cols'], rows_info['rows'])
        dest = f"{CATALOG}.{NS}.{tbl}"
        print(f"[{tbl}] appending {len(rows_info['rows'])} rows to {dest}")
        try:
            snapshot_id = append_iceberg_and_get_snapshot(dest, df)
        except Exception as e:
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute("UPDATE migration_history SET status='failed', notes=%s, finished_at=now() WHERE batch_id=%s AND relname=%s", (str(e), batch_id, tbl))
                conn.commit()
            conn.close()
            raise

        # Postgres 반영: 임시 테이블을 원본 타입으로 생성하여 INSERT 후 DELETE 실행
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE migration_history SET status=%s, iceberg_snapshot_id=%s, finished_at=now() WHERE batch_id=%s AND relname=%s", ('success', snapshot_id, batch_id, tbl))

                pk_cols = task['pk_columns']
                # 임시 stg 테이블을 원본과 동일한 타입으로 생성한 뒤 non-pk 컬럼 제거
                stg = create_temp_stg_like_table(cur, tbl, pk_cols)

                # pk_rows는 원시 파이썬 값으로 넣기 (타입 유지)
                pk_rows = []
                for row in rows_info['rows']:
                    rmap = dict(zip(rows_info['cols'], row))
                    pk_rows.append(tuple(rmap[c] for c in pk_cols))

                psycopg2.extras.execute_values(cur, f"INSERT INTO {stg} ({', '.join(pk_cols)}) VALUES %s", pk_rows, page_size=1000)

                cond = " AND ".join([f"{tbl}.{c} = s.{c}" for c in pk_cols])
                cur.execute(f"DELETE FROM {tbl} USING {stg} s WHERE {cond};")
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
