import psycopg2
import duckdb
import pandas as pd
import re
import requests
import time


# JAR 파일 경로를 설정 (코드와 동일한 디렉토리에 JAR 파일이 있음)
hadoop_aws_jar = "/Users/vldb/Downloads/p2i/hadoop-aws-3.3.4.jar"
aws_sdk_jar = "/Users/vldb/Downloads/p2i/aws-java-sdk-bundle-1.12.661.jar"
iceberg_jar = "/Users/vldb/Downloads/p2i/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
bundle_jar="/Users/vldb/Downloads/p2i/bundle-2.29.38.jar"
commons_jar="/Users/vldb/Downloads/p2i/commons-configuration2-2.11.0.jar"
postgresql_jar = "/Users/vldb/Downloads/p2i/postgresql-42.7.1.jar"

from pyspark.sql import SparkSession

# PostgreSQL 실행 관련 변수
PG_USER = "postgres"
PG_PASSWORD = "wnsdud318"
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "benchbase"

# AWS S3의 Iceberg 테이블 경로
S3_TABLE_PATH = "s3://vldb-000/iceberg/order_line"

# AWS S3 인증 정보
AWS_REGION = "ap-northeast-2"
AWS_ACCESS_KEY = "DUMMY_ACCESS_KEY_ID"
AWS_SECRET_KEY = "DUMMY_SECRET_KEY_VALUE"

# Iceberg REST Catalog 설정
# iceberg_table_name만 postgres의 테이블과 동일하게 바꿔주면 정상작동!
ICEBERG_NAMESPACE = "iceberg"
ICEBERG_TABLE_NAME = "order_line"
WAREHOUSE_PATH = "s3a://vldb-000/iceberg/"

def spark_query(query):
    from pyspark.sql import SparkSession
    import psycopg2

    spark = SparkSession.builder \
        .appName("S3AccessExample") \
        .config("spark.jars",
                f"{postgresql_jar},{hadoop_aws_jar},{aws_sdk_jar},{iceberg_jar}, {bundle_jar}, {commons_jar}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.hadoop.fs.s3a.access.key", "DUMMY_ACCESS_KEY_ID") \
        .config("spark.hadoop.fs.s3a.secret.key", "DUMMY_SECRET_KEY_VALUE") \
        .config("spark.hadoop.fs.s3a.endpoint", "https://s3.ap-norhteast-2amazonaws.com") \
        .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.rest.type", "rest") \
        .config("spark.sql.catalog.rest.uri", "http://0.0.0.0:8181") \
        .config("spark.sql.catalog.rest.warehouse", "s3a://vldb-000/iceberg/") \
        .getOrCreate()

    try:
        # 쿼리 변환 시간 포함
        start_time = time.perf_counter()

        converted_query = convert_query_to_spark_iceberg(query)
        df = spark.sql(converted_query)
        df.count()  # materialize

        end_time = time.perf_counter()
        print(f"✅ Spark 쿼리 실행 시간 (변환 포함): {end_time - start_time:.3f}초")
        return end_time - start_time

    except Exception as e:
        print("❌ Spark Query Error:", e)
        return None

    finally:
        spark.stop()



# PostgreSQL에서 쿼리 실행
def execute_postgres_query(query):
    try:
        conn = psycopg2.connect(
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        cur = conn.cursor()
        cur.execute(query)

        # ✅ 기존 뷰 삭제 후 다시 생성
        if query.strip().upper().startswith("SELECT"):
            cur.execute("DROP VIEW IF EXISTS pg_result;")  # 기존 뷰 삭제
            create_view_query = f"CREATE VIEW pg_result AS {query}"
            cur.execute(create_view_query)
            conn.commit()
            cur.close()
            conn.close()
            print("✅ PostgreSQL 뷰 `pg_result` 생성 완료")
            return True

        # ✅ SELECT가 아닌 경우 일반 쿼리 실행
        cur.execute(query)
        conn.commit()

        cur.close()
        conn.close()
        print("✅ PostgreSQL Query executed successfully.")
        return None

    except psycopg2.Error as e:
        print("❌ PostgreSQL Error:", e)
        return None


def extract_table_names(query):
    """
    사용자의 SQL 쿼리에서 테이블 이름을 자동으로 추출
    """
    # 정규식을 사용하여 FROM과 JOIN 뒤에 나오는 테이블 이름을 추출
    table_pattern = re.findall(r"(?i)(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)", query)
    return set(table_pattern)  # 중복 제거


def convert_query_to_iceberg(query):
    """
    사용자가 입력한 쿼리에서 테이블을 자동 변환하여 Iceberg 테이블을 대상으로 동작하도록 변경
    """
    table_names = extract_table_names(query)  # 사용자의 쿼리에서 테이블 이름 추출
    for table in table_names:

        # REST API에서 최신 Iceberg 테이블 정보 가져오기
        rest_catalog_url = f"http://0.0.0.0:8181/v1/namespaces/iceberg/tables/{table}"
        response = requests.get(rest_catalog_url).json()

        # 최신 메타데이터 파일 경로 추출
        metadata_location = response["metadata-location"]

        # FROM/JOIN + <table> + optional alias 패턴
        pattern = rf"(?i)(FROM|JOIN)\s+{table}(\s+(AS\s+)?(\w+))?"

        def replacer(match):
            keyword = match.group(1)  # FROM or JOIN
            alias = match.group(4) if match.group(4) else table  # ol, c, i 등 or default = table name
            return f"{keyword} (SELECT * FROM iceberg_scan('{metadata_location}')) AS {alias}"

        query = re.sub(pattern, replacer, query)

    return query

def convert_query_to_spark_iceberg(query):
    """
    Spark용으로 테이블 이름을 rest.catalog.namespace.table 형식으로 변환 (alias 포함)
    """
    table_names = extract_table_names(query)

    for table in table_names:
        pattern = rf"(?i)(FROM|JOIN)\s+{table}(\s+(AS\s+)?(\w+))?"

        def replacer(match):
            keyword = match.group(1)  # FROM or JOIN
            alias = match.group(4) if match.group(4) else table
            return f"{keyword} rest.{ICEBERG_NAMESPACE}.{table} AS {alias}"

        query = re.sub(pattern, replacer, query)

    return query




### ✅ 2️⃣ DuckDB에서 Iceberg 쿼리 실행 & PostgreSQL 데이터와 병합
def execute_duckdb_olap_with_timer(query):
    import time
    try:
        conn = duckdb.connect()
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute("INSTALL aws; LOAD aws;")
        conn.execute("INSTALL iceberg; LOAD iceberg;")
        conn.execute("INSTALL postgres_scanner; LOAD postgres_scanner;")

        conn.execute(f"""
            SET s3_region='{AWS_REGION}';
            SET s3_access_key_id='{AWS_ACCESS_KEY}';
            SET s3_secret_access_key='{AWS_SECRET_KEY}';
        """)

        # DuckDB + 변환 + PostgreSQL 포함한 전체 시간 측정 시작
        start_time = time.perf_counter()


        # 2️⃣ 사용자 쿼리 → Iceberg 대상 쿼리로 변환
        converted_query = convert_query_to_iceberg(query)

        # 3️⃣ DuckDB 실행 + materialization
        result_df = conn.execute(converted_query).fetchdf()

        end_time = time.perf_counter()
        elapsed_time = end_time - start_time

        print(f"✅ DuckDB (전체 포함) 실행 시간: {elapsed_time:.3f}초")
        return result_df, elapsed_time

    except Exception as e:
        print("❌ DuckDB Error:", e)
        return None, None



# 실행
if __name__ == "__main__":
    user_query = input("💬 비교할 SQL 쿼리를 입력하세요: ").strip()

    print("\n🚀 DuckDB 실행 중...")
    duckdb_result, duckdb_time = execute_duckdb_olap_with_timer(user_query)
    print(duckdb_result)

    print("\n🚀 Spark 실행 중...")
    spark_time = spark_query(user_query)

    print("\n⏱️ 최종 비교:")
    print(f"🔹 DuckDB (변환+실행): {duckdb_time:.3f}초")
    print(f"🔸 Spark (변환+실행):  {spark_time:.3f}초")

