#!/usr/bin/env python3
"""
tpch_to_iceberg_ch.py – DuckDB ➜ Spark ➜ Iceberg (CHBenCHmark 스타일 이름 적용)
"""

import duckdb
import os
from pyspark.sql import SparkSession

# ───── JAR 경로 설정 ─────
hadoop_aws_jar = "/home/vldb/jun/p2i/hadoop-aws-3.3.4.jar"
aws_sdk_jar = "/home/vldb/jun/p2i/aws-java-sdk-bundle-1.12.661.jar"
iceberg_jar = "/home/vldb/jun/p2i/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
bundle_jar = "/home/vldb/jun/p2i/bundle-2.29.38.jar"
commons_jar = "/home/vldb/jun/p2i/commons-configuration2-2.11.0.jar"

# ───── AWS & Iceberg 설정 ─────
AWS_ACCESS = "DUMMY_ACCESS_KEY_ID"
AWS_SECRET = "DUMMY_SECRET_KEY_VALUE"
WAREHOUSE = "s3a://vldb-000/iceberg/"

# ───── DuckDB에서 TPC-H SF1 데이터 생성 ─────
print("📦 Generating TPC-H SF1 data...")
con = duckdb.connect()
con.execute("INSTALL tpch; LOAD tpch; CALL dbgen(sf=1);")

# ───── 테이블 이름 매핑 (CH prefix) ─────
tpch_map = {
    "region": "ch_region",
    "nation": "ch_nation",
    "supplier": "ch_supplier",
    "customer": "ch_customer",
    "part": "ch_part",
    "partsupp": "ch_partsupp",
    "orders": "ch_orders",
    "lineitem": "ch_lineitem"
}

# ───── Parquet로 저장 ─────
parquet_dir = "/tmp/tpch_parquet_ch"
os.makedirs(parquet_dir, exist_ok=True)
con.execute(f"SET temp_directory='{parquet_dir}';")

for src_tbl, ch_tbl in tpch_map.items():
    out_path = f"{parquet_dir}/{ch_tbl}.parquet"
    con.execute(f"COPY {src_tbl} TO '{out_path}' (FORMAT 'parquet');")

print("✅ Parquet export 완료")

# ───── SparkSession 시작 ─────
spark = (SparkSession.builder
    .appName("TPC-H CHBenCHmark to Iceberg")
    .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_jar},{iceberg_jar},{bundle_jar},{commons_jar}")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.rest.type", "rest")
    .config("spark.sql.catalog.rest.uri", "http://0.0.0.0:8181")
    .config("spark.sql.catalog.rest.warehouse", WAREHOUSE)
    .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET)
    .config("spark.hadoop.fs.s3a.endpoint", "https://s3.ap-northeast-2.amazonaws.com")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "4g")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ───── Iceberg로 전송 ─────
for src_tbl, ch_tbl in tpch_map.items():
    print(f"🚚 {src_tbl} -> iceberg.rest.{ch_tbl}")
    df = spark.read.parquet(f"{parquet_dir}/{ch_tbl}.parquet")
    dest = f"rest.iceberg.{ch_tbl}"

    if not spark.catalog.tableExists(dest):
        coldef = ", ".join(f"{c} {t.upper()}" for c, t in df.dtypes)
        spark.sql(f"CREATE TABLE {dest} ({coldef}) USING iceberg LOCATION '{WAREHOUSE}{ch_tbl}'")

    df.writeTo(dest).append()

print("🎉 모든 CH 테이블이 Iceberg에 저장 완료")
spark.stop()
