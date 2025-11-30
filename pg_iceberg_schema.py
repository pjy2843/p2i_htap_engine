#포스트그레스에 있는 테이블들과 동일한 스키마를 지닌 빈 아이스버그 테이블을 생성하는 스크립트

import psycopg2
from pyspark.sql import SparkSession

# JAR 파일 경로를 설정 (코드와 동일한 디렉토리에 JAR 파일이 있음)
hadoop_aws_jar = "/home/vldb/jun/p2i/hadoop-aws-3.3.4.jar"
aws_sdk_jar = "/home/vldb/jun/p2i/aws-java-sdk-bundle-1.12.661.jar"
iceberg_jar = "/home/vldb/jun/p2i/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
bundle_jar="/home/vldb/jun/p2i/bundle-2.29.38.jar"
commons_jar="/home/vldb/jun/p2i/commons-configuration2-2.11.0.jar"

# SparkSession 생성
# 엑세스키, 시크릿엑세스키, 웨어하우스 입력하세요
spark = SparkSession.builder \
    .appName("S3AccessExample") \
    .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_jar},{iceberg_jar},{bundle_jar},{commons_jar}") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.hadoop.fs.s3a.access.key", "DUMMY_ACCESS_KEY_ID") \
    .config("spark.hadoop.fs.s3a.secret.key", "DUMMY_SECRET_KEY_VALUE") \
    .config("spark.hadoop.fs.s3a.endpoint", "https://s3.ap-northeast-2.amazonaws.com") \
    .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.rest.type", "rest") \
    .config("spark.sql.catalog.rest.uri", "http://0.0.0.0:8181") \
    .config("spark.sql.catalog.rest.warehouse", "s3a://vldb-000/iceberg/") \
    .config("spark.sql.catalog.rest.write.version-hint.enabled", "true") \
    .getOrCreate()


# PostgreSQL 연결 정보
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "chbench"
PG_USER = "postgres"
PG_PASSWORD = "postgres"

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


# 실행
def get_postgres_table_list():
    conn = psycopg2.connect(
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
    """)
    result = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return result

# 실행 루프
for table_name in get_postgres_table_list():
    columns = get_postgres_schema(table_name)
    iceberg_create_sql = generate_iceberg_create_table(table_name, columns)
    print(f"▶️ Creating Iceberg table for: {table_name}")
    spark.sql(iceberg_create_sql).show()






#확인

# spark.sql("drop table rest.iceberg.customer;").show()
#
# spark.sql("show namespaces in rest;").show()
#
# spark.sql(iceberg_create_sql).show()
#
# spark.sql("select * from rest.iceberg.oorder.manifests;").show()
#
# spark.sql("show Tables in rest.iceberg;").show()
#
# spark.sql("DESCRIBE TABLE rest.iceberg.order_line;").show()

