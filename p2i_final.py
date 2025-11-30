import psycopg2
import duckdb
import pandas as pd
import re
import requests
from pyspark.sql import SparkSession

# JAR 파일 경로를 설정 (코드와 동일한 디렉토리에 JAR 파일이 있음)
hadoop_aws_jar = "/Users/vldb/Downloads/p2i/hadoop-aws-3.3.4.jar"
aws_sdk_jar = "/Users/vldb/Downloads/p2i/aws-java-sdk-bundle-1.12.661.jar"
iceberg_jar = "/Users/vldb/Downloads/p2i/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
bundle_jar="/Users/vldb/Downloads/p2i/bundle-2.29.38.jar"
commons_jar="/Users/vldb/Downloads/p2i/commons-configuration2-2.11.0.jar"
postgresql_jar = "/Users/vldb/Downloads/p2i/postgresql-42.7.1.jar"


# PostgreSQL 실행 관련 변수
PG_USER = "postgres"
PG_PASSWORD = "wnsdud318"
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "benchbase"
jdbc_url= f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# AWS S3 인증 정보
AWS_REGION = "ap-northeast-2"
AWS_ACCESS_KEY = "DUMMY_ACCESS_KEY_ID"
AWS_SECRET_KEY = "DUMMY_SECRET_KEY_VALUE"

# Iceberg REST Catalog 설정
ICEBERG_NAMESPACE = "iceberg"
WAREHOUSE_PATH = "s3a://vldb-000/iceberg/"

# PostgreSQL → Iceberg 타입 매핑
PG_TO_ICEBERG_TYPE_MAP = {
    "integer": "INT",
    "bigint": "BIGINT",
    "double precision": "DOUBLE",
    "real": "FLOAT",
    "numeric": "DECIMAL(10,2)",
    "varchar": "STRING",
    "text": "STRING",
    "boolean": "BOOLEAN",
    "timestamp without time zone": "TIMESTAMP",
    "date": "DATE",
}


# PostgreSQL에서 테이블 스키마 가져오기
def get_postgres_schema(table_name):
    conn = psycopg2.connect(
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s AND table_schema = 'public';
    """, (table_name,))

    columns = cur.fetchall()
    cur.close()
    conn.close()

    return columns

# Iceberg 테이블 생성 SQL 생성
def generate_iceberg_create_table(table_name, columns):
    column_definitions = []

    for col_name, col_type in columns:
        iceberg_type = PG_TO_ICEBERG_TYPE_MAP.get(col_type, "STRING")  # 기본값 STRING
        column_definitions.append(f"{col_name} {iceberg_type}")

    column_definitions_str = ",\n    ".join(column_definitions)

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS rest.{ICEBERG_NAMESPACE}.{table_name} (
        {column_definitions_str}
    ) USING iceberg
    LOCATION '{WAREHOUSE_PATH}{table_name}/';
    """

    return create_table_sql

# FK로 조회한 자식 테이블의 스키마, 테이블명 분리
def parse_schema_and_table(full_name: str):
    parts = full_name.split(".")
    if len(parts) == 2:
        return parts[0], parts[1]
    else:
        return "public", full_name

def drop_fk_constraints(table_name):
    """
    주어진 테이블을 참조하는 FK constraint를 전부 찾아서 Drop하는 함수
    """
    conn = psycopg2.connect(
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )
    cur = conn.cursor()

    get_fk_sql = """
    SELECT conname, conrelid::regclass::text
    FROM pg_constraint
    WHERE contype = 'f'
      AND confrelid = %s::regclass;
    """
    cur.execute(get_fk_sql, (table_name,))
    fks = cur.fetchall()

    for conname, child_table in fks:
        drop_sql = f"ALTER TABLE {child_table} DROP CONSTRAINT {conname};"
        print(f"👉 Dropping FK: {drop_sql}")
        cur.execute(drop_sql)

    conn.commit()
    cur.close()
    conn.close()

def get_primary_key_columns(table_name):
    with psycopg2.connect(
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary;
            """, (table_name,))
            return [row[0] for row in cur.fetchall()]


def run_spark_pipeline(query, table_name, spark):
    # 테이블 데이터 읽기
    # ****** 단일 테이블의 경우 병렬 읽기를 해서 성능 개선 가능성있음.
    spark_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"({query}) AS subquery") \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # Iceberg에 append
    iceberg_table = f"rest.iceberg.{table_name.replace('.', '_')}"
    if not spark._jsparkSession.catalog().tableExists(iceberg_table):
        columns = get_postgres_schema(table_name)
        create_sql = generate_iceberg_create_table(table_name, columns)
        spark.sql(create_sql)

    spark_df.writeTo(iceberg_table).append()

    # 테이블 삭제: primary key만 저장하고 있는 Staging table 만들어서 삭제
    # 전송량이 수만~수십만 row가 초과할 경우 바로 삭제하는 것이 오히려 느림
    key_columns = get_primary_key_columns(table_name)
    staging_table = f"staging_delete_{table_name.replace('.', '_')}"
    spark_df.select(*key_columns).distinct().write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", staging_table) \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    # DELETE 수행 (staging table 기반)
    delete_sql = f"DELETE FROM {table_name} USING {staging_table} WHERE " + \
                 " AND ".join([f"{table_name}.{col} = {staging_table}.{col}" for col in key_columns])

    conn = psycopg2.connect(
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )
    cur = conn.cursor()
    cur.execute(delete_sql)
    conn.commit()
    cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
    conn.commit()
    cur.close()
    conn.close()

# # spark를 통해 iceberg metadata aware하게 데이터를 fetch해온 다음 arrow로 duckdb에 전달
# def fetch_iceberg_filtered_arrow(table_name: str, predicate: str, spark: SparkSession):
#     """
#     Iceberg에서 predicate 기반으로 필요한 데이터만 읽고
#     Pandas/Arrow로 변환하여 DuckDB로 넘기기 위한 함수
#     """
#     full_table = f"{ICEBERG_NAMESPACE}.{table_name}"
#
#     query = f"""
#         SELECT * FROM {full_table}
#         WHERE {predicate}
#     """
#
#     print(f"🔥 Executing Iceberg filter query on Spark: {query}")
#     df = spark.sql(query)
#
#     # return either:
#     # return df.toPandas()
#     return df.to_arrow_table()



# # OLTP/OLAP 쿼리 분류 함수
# def classify_query(query):
#     query = query.strip().upper()
#
#     # OLTP 패턴 (INSERT, UPDATE, DELETE만 포함)
#     oltp_patterns = [r"^\s*(INSERT|UPDATE|DELETE|REPLACE)\s"]
#
#     # OLAP 패턴 (SELECT + 분석 연산 포함)
#     olap_patterns = [
#         r"\s(GROUP\sBY|HAVING|ORDER\sBY|DISTINCT|LIMIT)\s",
#         r"\s(SUM|AVG|COUNT|MAX|MIN)\s*\(",
#         r"\sJOIN\s"
#     ]
#
#     # OLTP 판별
#     for pattern in oltp_patterns:
#         if re.search(pattern, query):
#             return "OLTP"
#
#     # OLAP 판별
#     for pattern in olap_patterns:
#         if re.search(pattern, query):
#             return "OLAP"
#
#     # SELECT는 기본적으로 OLAP으로 처리
#     if query.startswith("SELECT"):
#         return "OLAP"
#
#     return "UNKNOWN"


# # PostgreSQL에서 쿼리 실행
# def execute_postgres_query(query):
#     try:
#         conn = psycopg2.connect(
#             dbname=PG_DATABASE,
#             user=PG_USER,
#             password=PG_PASSWORD,
#             host=PG_HOST,
#             port=PG_PORT
#         )
#         cur = conn.cursor()
#         cur.execute(query)
#
#         # ✅ 기존 뷰 삭제 후 다시 생성
#         if query.strip().upper().startswith("SELECT"):
#             cur.execute("DROP VIEW IF EXISTS pg_result;")  # 기존 뷰 삭제
#             create_view_query = f"CREATE VIEW pg_result AS {query}"
#             cur.execute(create_view_query)
#             conn.commit()
#             cur.close()
#             conn.close()
#             print("✅ PostgreSQL 뷰 `pg_result` 생성 완료")
#             return True
#
#         # ✅ SELECT가 아닌 경우 일반 쿼리 실행
#         cur.execute(query)
#         conn.commit()
#
#         cur.close()
#         conn.close()
#         print("✅ PostgreSQL Query executed successfully.")
#         return None
#
#     except psycopg2.Error as e:
#         print("❌ PostgreSQL Error:", e)
#         return None


def extract_table_names(query):
    """
    사용자의 SQL 쿼리에서 테이블 이름을 자동으로 추출
    """
    # 정규식을 사용하여 FROM과 JOIN 뒤에 나오는 테이블 이름을 추출
    table_pattern = re.findall(r"(?i)(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)", query)
    return set(table_pattern)  # 중복 제거


# def convert_query_to_iceberg(query):
#     """
#     사용자가 입력한 쿼리에서 테이블을 자동 변환하여 Iceberg 테이블을 대상으로 동작하도록 변경
#     """
#     table_names = extract_table_names(query)  # 사용자의 쿼리에서 테이블 이름 추출
#     for table in table_names:
#
#         # REST API에서 최신 Iceberg 테이블 정보 가져오기
#         rest_catalog_url = f"http://0.0.0.0:8181/v1/namespaces/iceberg/tables/{table}"
#         response = requests.get(rest_catalog_url).json()
#
#         # 최신 메타데이터 파일 경로 추출
#         metadata_location = response["metadata-location"]
#
#         # FROM/JOIN + <table> + optional alias 패턴
#         pattern = rf"(?i)(FROM|JOIN)\s+{table}(\s+(AS\s+)?(\w+))?"
#
#         def replacer(match):
#             keyword = match.group(1)  # FROM or JOIN
#             alias = match.group(4) if match.group(4) else table  # ol, c, i 등 or default = table name
#             return f"{keyword} (SELECT * FROM iceberg_scan('{metadata_location}')) AS {alias}"
#
#         query = re.sub(pattern, replacer, query)
#
#     return query
#
#
# ### ✅ 2️⃣ DuckDB에서 Iceberg 쿼리 실행 & PostgreSQL 데이터와 병합
# def execute_duckdb_olap(query):
#     """DuckDB에서 Iceberg 테이블을 쿼리 실행"""
#     try:
#         conn = duckdb.connect()
#         conn.execute("INSTALL httpfs; LOAD httpfs;")
#         conn.execute("INSTALL aws; LOAD aws;")
#         conn.execute("INSTALL iceberg; LOAD iceberg;")
#         conn.execute("INSTALL postgres_scanner; LOAD postgres_scanner;")
#
#         conn.execute(f"""
#             SET s3_region='{AWS_REGION}';
#             SET s3_access_key_id='{AWS_ACCESS_KEY}';
#             SET s3_secret_access_key='{AWS_SECRET_KEY}';
#         """)
#
#         #OLAP에 데이터가 더 많다는 가정
#
#         conn.execute(f"""
#             ATTACH 'dbname={PG_DATABASE} user={PG_USER} host={PG_HOST} password={PG_PASSWORD} port={PG_PORT}' AS postgres (TYPE POSTGRES);
#         """)
#
#         # view가 생성되었을 경우, pg_flag is not none, pg_result라는 이름의 뷰 생성
#         pg_flag = execute_postgres_query(query)
#
#         print(conn.execute("select * from postgres.pg_result"))
#
#         # ✅ 사용자의 쿼리를 Iceberg 테이블 대상으로 변환
#         converted_query = convert_query_to_iceberg(query)
#
#         print(converted_query)
#
#         #view가 생성되었을 경우, union all query로 변경
#         if pg_flag is True:
#             # ✅ DuckDB에서 PostgreSQL 데이터를 pg_result로 조인 (customer 아님), attach할 때 as postgres로 설정했으므로 postgres.pg_result
#             converted_query = converted_query.strip().rstrip(";")
#             converted_query += " UNION ALL Select * from postgres.pg_result;"
#             print("join 추가:", converted_query)
#
#         # ✅ DuckDB에서 쿼리 실행
#         iceberg_df = conn.execute(converted_query).fetchdf()
#         return iceberg_df
#
#     except Exception as e:
#         print("❌ DuckDB Error:", e)
#         return None


# 사용자 입력을 받아 쿼리를 실행
def execute_query(query):
    query_type = classify_query(query)
    print(f"🔍 Query Type: {query_type}")

    if query_type == "OLTP":
        # OLTP는 PostgreSQL에서만 실행
        return execute_postgres_query(query)

    elif query_type == "OLAP":
        # OLAP 쿼리: PostgreSQL과 DuckDB에서 각각 실행 후 병합(duckdb내에서)
        result_df = execute_duckdb_olap(query)  # DuckDB (Iceberg) 결과
        return result_df

    else:
        print("❌ Unknown query type.")
        return None


# 실행
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("S3AccessExample") \
        .config("spark.jars",
                f"{postgresql_jar},{hadoop_aws_jar},{aws_sdk_jar},{iceberg_jar}, {bundle_jar}, {commons_jar}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", "https://s3.ap-northeast-2.amazonaws.com") \
        .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.rest.type", "rest") \
        .config("spark.sql.catalog.rest.uri", "http://0.0.0.0:8181") \
        .config("spark.sql.catalog.rest.warehouse", "s3a://vldb-000/iceberg/") \
        .getOrCreate()

    queries = {
        # TPC-C 테이블
        # "customer": "SELECT * FROM customer WHERE c_id > 100",
        # # 행 10개짜리 "district": "SELECT * FROM district WHERE d_id > 1",
        # # "hhistory": "SELECT * FROM hhistory WHERE h_c_id > 100",
        # "item": "SELECT * FROM item WHERE i_id > 100",
        # "oorder": "SELECT * FROM oorder WHERE o_id > 100",
        # "order_line": "SELECT * FROM order_line WHERE ol_i_id > 100",
        # "stock": "SELECT * FROM stock WHERE s_i_id > 100"
        # # 행 1개짜리 "warehouse": "SELECT * FROM warehouse WHERE w_id > 100"
        # # neworder 제외

        # TPC-H 테이블: 아래 쿼리 실행해서 아이스버그로 저장, 나머지는 포스트그레스에 그대로 있음
        "customer": "select * from customer where c_custkey>100",
        "lineitem": "select * from lineitem where l_orderkey>100",
        "orders": "select * from orders where o_orderkey>100",
        "part": "select * from part where p_partkey>100",
        "partsupp": "select * from partsupp where ps_suppkey>100",
        # nation(24), region(5), supplier(1000)은 포스트그레스에 전부다 저장
    }

    for table_name, query in queries.items():
        print(f"🚀 Processing {table_name}: {query}")
        try:
            drop_fk_constraints(table_name)
            run_spark_pipeline(query, table_name, spark)
        except Exception as e:
            print(f"❌ Error processing {table_name}: {e}")

    # while True:
        # user_query = input("💬 Enter your SQL query (or type 'exit' to quit): ").strip()
        # if user_query.lower() == "exit":
        #     break

        # table_names = extract_table_names(user_query)
        # table_name = sorted(table_names)[0]

        # drop_fk_constraints(table_name)  # FK 먼저 삭제
        # run_spark_pipeline(user_query, table_name, spark)




        # result_df = execute_query(user_query)
        # if result_df is not None:
        #     print(result_df)
