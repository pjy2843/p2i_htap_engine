#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Postgres ➜ Iceberg 증분 전송 (개선된 버전)
- psycopg2를 사용해 데이터를 직접 읽어 Spark의 JDBC 및 트랜잭션 가시성 문제를 우회합니다.
- sync_policy 및 sync_watermark 메타 테이블을 사용하여 마이그레이션 대상을 동적으로 결정합니다.
- hhistory 테이블 마이그레이션 로직을 추가했습니다.
"""

import os, time, psycopg2, psycopg2.extras
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# ───── 0. 환경 설정 ─────
PG_USER, PG_PASSWORD = "postgres", "postgres"
PG_HOST, PG_PORT     = "localhost", "5432"
PG_DB                = "chbench"

AWS_REGION   = "ap-northeast-2"
AWS_ACCESS   = "DUMMY_ACCESS_KEY_ID"
AWS_SECRET   = "DUMMY_SECRET_KEY_VALUE"

DSN       = f"dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} host={PG_HOST} port={PG_PORT}"
CATALOG   = "rest"
NS        = "iceberg"
WAREHOUSE = "s3a://vldb-000/iceberg/"

# ───── 1. SparkSession 설정 ─────
hadoop_aws_jar = "/home/vldb/jun/p2i/hadoop-aws-3.3.4.jar"
aws_sdk_jar    = "/home/vldb/jun/p2i/aws-java-sdk-bundle-1.12.661.jar"
iceberg_jar    = "/home/vldb/jun/p2i/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
bundle_jar     = "/home/vldb/jun/p2i/bundle-2.29.38.jar"
commons_jar    = "/home/vldb/jun/p2i/commons-configuration2-2.11.0.jar"
postgresql_jar = "/home/vldb/jun/p2i/postgresql-42.7.1.jar"

spark = SparkSession.builder \
    .appName("migration-improved") \
    .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_jar},{iceberg_jar},{bundle_jar},{commons_jar},{postgresql_jar}") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET) \
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com") \
    .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.rest.type", "rest") \
    .config("spark.sql.catalog.rest.uri", "http://localhost:8181") \
    .config("spark.sql.catalog.rest.warehouse", WAREHOUSE) \
    .config("spark.sql.catalog.rest.write.version-hint.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NS}")

# ───── 2. PostgreSQL 헬퍼 함수 ─────
def fetch_all(sql, params=()):
    with psycopg2.connect(DSN) as c, c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def fetch_one(sql, params=()):
    with psycopg2.connect(DSN) as c, c.cursor() as cur:
        cur.execute(sql, params)
        r = cur.fetchone()
        return r[0] if r else None

def exec_sql(sql, params=()):
    with psycopg2.connect(DSN) as c, c.cursor() as cur:
        cur.execute(sql, params)
        c.commit()

def setup_meta_tables():
    """마이그레이션에 필요한 메타 테이블들을 생성하고 정책을 정의합니다."""
    exec_sql("""
        CREATE TABLE IF NOT EXISTS sync_policy (
            relname text PRIMARY KEY,
            category text CHECK (category IN ('append_only','update_once')),
            ts_column text NOT NULL, -- 증분 기준이 되는 타임스탬프 컬럼
            pk_columns text[] NOT NULL -- 삭제 시 사용할 기본 키 컬럼들
        );
        CREATE TABLE IF NOT EXISTS sync_watermark(
            relname   text PRIMARY KEY,
            last_ts   timestamptz NOT NULL DEFAULT '1970-01-01 UTC'
        );
    """)
    # 정책 정의 (hhistory 추가)
    exec_sql("""
        INSERT INTO sync_policy (relname, category, ts_column, pk_columns) VALUES 
        ('orders', 'update_once', 'o_entry_d', ARRAY['o_w_id', 'o_d_id', 'o_id']),
        ('order_line', 'update_once', 'ol_delivery_d', ARRAY['ol_w_id', 'ol_d_id', 'ol_o_id', 'ol_number']),
        ('hhistory', 'append_only', 'h_date', ARRAY['h_c_id', 'h_c_d_id', 'h_c_w_id', 'h_date']) -- hhistory는 뚜렷한 PK가 없어 타임스탬프 포함
        ON CONFLICT (relname) DO UPDATE SET 
            category = EXCLUDED.category,
            ts_column = EXCLUDED.ts_column,
            pk_columns = EXCLUDED.pk_columns;
    """)

# ───── 3. 증분 데이터 조회 쿼리 (hhistory 추가) ─────
def q_hhistory_base(last_ts, cutoff):
    return f"""
      SELECT * FROM hhistory
      WHERE (h_date AT TIME ZONE 'Asia/Seoul') > '{last_ts}'
        AND (h_date AT TIME ZONE 'Asia/Seoul') <= '{cutoff}'
    """

def q_order_line_base(last_ts, cutoff):
    return f"""
      SELECT * FROM order_line
      WHERE ol_delivery_d IS NOT NULL
        AND (ol_delivery_d AT TIME ZONE 'Asia/Seoul') > '{last_ts}'
        AND (ol_delivery_d AT TIME ZONE 'Asia/Seoul') <= '{cutoff}'
    """

def q_orders_base(last_ts, cutoff):
    return f"""
      SELECT o.* FROM orders o
      JOIN (
        SELECT ol_w_id AS o_w_id, ol_d_id AS o_d_id, ol_o_id AS o_id,
               MAX(ol_delivery_d AT TIME ZONE 'Asia/Seoul') AS closed_at
        FROM order_line WHERE ol_delivery_d IS NOT NULL
        GROUP BY 1,2,3
      ) x USING (o_w_id, o_d_id, o_id)
      WHERE x.closed_at > '{last_ts}' AND x.closed_at <= '{cutoff}'
    """

# ───── 4. Spark 데이터프레임 함수 (기존과 동일) ─────
def read_df(sql_text):
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text)
            if not cur.description: return spark.createDataFrame([], StructType([]))
            col_names = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            if not rows:
                schema = StructType([StructField(name, StringType(), True) for name in col_names])
                return spark.createDataFrame([], schema=schema)
            else:
                return spark.createDataFrame(rows, schema=col_names)

def append_iceberg(dest, df):
    try:
        df.coalesce(1).writeTo(dest).append()
    except Exception:
        df.createOrReplaceTempView("__src_for_insert")
        spark.sql(f"INSERT INTO {dest} SELECT * FROM __src_for_insert")
        spark.catalog.dropTempView("__src_for_insert")
    spark.sql(f"REFRESH TABLE {dest}")

def df_pk_rows(df, pk_cols):
    if df.rdd.isEmpty(): return []
    it = df.select(*pk_cols).distinct().toLocalIterator()
    return [tuple(row[c] for c in pk_cols) for row in it]

# ───── 5. 메인 배치 실행 함수 (동적 로직으로 변경) ─────
def run_batch(delay_minutes=1):
    setup_meta_tables()
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=delay_minutes)
    
    tasks = fetch_all("SELECT * FROM sync_policy")
    dfs = {}

    try:
        print(f"DEBUG: Reading data up to cutoff: {cutoff.isoformat()}")
        for task in tasks:
            tbl = task['relname']
            last_from_db = fetch_one("SELECT last_ts FROM sync_watermark WHERE relname=%s", (tbl,))
            last = last_from_db.astimezone(timezone.utc) if last_from_db else datetime(1970, 1, 1, tzinfo=timezone.utc)
            
            print(f"DEBUG: Watermark for {tbl} is {last.isoformat()}")

            # 쿼리 동적 선택
            if tbl == "order_line": sql = q_order_line_base(last, cutoff)
            elif tbl == "orders": sql = q_orders_base(last, cutoff)
            elif tbl == "hhistory": sql = q_hhistory_base(last, cutoff)
            else: 
                print(f"WARN: No query generator for table {tbl}. Skipping.")
                continue

            df = read_df(sql)
            count = df.count()
            print(f"DEBUG: Found {count} rows for {tbl}.")
            dfs[tbl] = df
        
        # Iceberg에 데이터 Append
        for task in tasks:
            tbl = task['relname']
            if tbl not in dfs or dfs[tbl].rdd.isEmpty(): continue
            dest = f"{CATALOG}.{NS}.{tbl}"
            print(f"DEBUG: Appending data for {tbl} to Iceberg...")
            append_iceberg(dest, dfs[tbl])
            print(f"DEBUG: Append for {tbl} finished.")

        # PostgreSQL에서 데이터 삭제 및 워터마크 업데이트 (단일 트랜잭션)
        with psycopg2.connect(DSN) as conn:
            conn.autocommit = False
            try:
                for task in tasks:
                    tbl = task['relname']
                    if tbl not in dfs or dfs[tbl].rdd.isEmpty(): continue
                    
                    pk_cols = task['pk_columns']
                    pk_rows = df_pk_rows(dfs[tbl], pk_cols)

                    if pk_rows:
                        with conn.cursor() as cur:
                            # hhistory의 PK는 타입이 다양하므로 임시 테이블 생성 시 타입을 지정해야 함.
                            # 여기서는 간단하게 text로 처리하지만, 실제로는 policy 테이블에 타입 정보도 넣으면 좋음.
                            cols_def = ", ".join(f"{c} text" for c in pk_cols)
                            cur.execute(f"CREATE TEMP TABLE stg_pk_{tbl} ({cols_def}) ON COMMIT DROP;")
                            psycopg2.extras.execute_values(
                                cur, f"INSERT INTO stg_pk_{tbl} ({', '.join(pk_cols)}) VALUES %s",
                                pk_rows, page_size=1000
                            )
                            cond = " AND ".join([f"{tbl}.{c}::text=s.{c}" for c in pk_cols])
                            cur.execute(f"DELETE FROM {tbl} USING stg_pk_{tbl} s WHERE {cond};")

                with conn.cursor() as cur:
                    for task in tasks:
                        tbl = task['relname']
                        cur.execute("""
                          INSERT INTO sync_watermark(relname, last_ts) VALUES (%s, %s)
                          ON CONFLICT(relname) DO UPDATE SET last_ts = EXCLUDED.last_ts
                        """, (tbl, cutoff))
                
                conn.commit()
                print("DEBUG: >>>>> PostgreSQL commit successful! Watermark updated. <<<<<")
            except Exception as e:
                conn.rollback()
                print(f"DEBUG: !!!! Exception during PostgreSQL commit. Rolling back: {e} !!!!")
                raise

    except Exception as e:
        print(f"DEBUG: !!!!!!! An unexpected error occurred: {e} !!!!!!!")
        import traceback
        traceback.print_exc()

# ───── 6. 메인 루프 (기존과 동일) ─────
if __name__ == "__main__":
    BENCH_TIME_SEC = int(os.getenv("BENCH_TIME_SEC", "700"))
    INTERVAL_SEC   = int(os.getenv("INTERVAL_SEC",   "120"))
    DELAY_MINUTES  = int(os.getenv("DELAY_MINUTES",  "1"))

    start = time.time()
    i = 0
    try:
        while time.time() - start < BENCH_TIME_SEC:
            i += 1
            print(f"\n[migration] #{i} run_batch start  delay={DELAY_MINUTES}m")
            run_batch(delay_minutes=DELAY_MINUTES)
            print(f"[migration] #{i} run_batch done")
            time.sleep(INTERVAL_SEC)
    finally:
        try:
            spark.stop()
        except: pass

