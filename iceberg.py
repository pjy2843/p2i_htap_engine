
# JAR 파일 경로를 설정 (코드와 동일한 디렉토리에 JAR 파일이 있음)
hadoop_aws_jar = "/Users/vldb/Downloads/석사학위논문/hadoop-aws-3.3.1.jar"
aws_sdk_jar = "/Users/vldb/Downloads/석사학위논문/aws-java-sdk-bundle-1.11.901.jar"
iceberg_jar = "/Users/vldb/Downloads/석사학위논문/iceberg-spark-runtime-3.3_2.12-0.14.0.jar"

from pyspark.sql import SparkSession

# SparkSession 생성
# 엑세스키, 시크릿엑세스키, 웨어하우스 입력하세요
spark = SparkSession.builder \
    .appName("S3AccessExample") \
    .config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_jar},{iceberg_jar}") \
    .config("spark.hadoop.fs.s3a.access.key", "DUMMY_ACCESS_KEY_ID") \
    .config("spark.hadoop.fs.s3a.secret.key", "DUMMY_SECRET_KEY_VALUE") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://vldb-000") \
    .getOrCreate()


# Iceberg 테이블 생성 예시 #spark_catalog.<namespace>.<table_name> --> namespace는 데이터베이스 또는 스키마의 역할을 한다고 이해하면 됨
# spark.sql("""
# CREATE TABLE spark_catalog.iceberg.orders (
#     order_id INTEGER,
#     customer_id INTEGER NOT NULL,
#     order_status VARCHAR(20) NOT NULL,
#     total_amount DECIMAL(10,2) NOT NULL,
#     last_updated TIMESTAMP
# ) USING iceberg
# """)

# Iceberg 테이블 확인 예제
spark.sql("SHOW TABLES IN spark_catalog.default").show()

# 1. 아이스버그 테이블에 직접 insert 하는 방법
spark.sql("""
INSERT INTO spark_catalog.iceberg.orders (order_id, customer_id, order_status, total_amount, last_updated) VALUES
    (1, 101, 'pending', 299.99, CURRENT_TIMESTAMP),
    (2, 102, 'processing', 1250.50, CURRENT_TIMESTAMP),
    (3, 103, 'shipped', 89.99, CURRENT_TIMESTAMP),
    (4, 104, 'delivered', 499.99, CURRENT_TIMESTAMP),
    (5, 105, 'cancelled', 750.00, CURRENT_TIMESTAMP)
""")


# 2.Schema evolution
# spark.sql("""
# ALTER TABLE spark_catalog.iceberg.my_table
# ADD COLUMNS (email STRING)
# """)

# spark.sql("""
# INSERT INTO spark_catalog.iceberg.my_table (id, name, age, email)
# VALUES 
#     (4, 'David', 40, 'david@example.com'),
#     (5, 'Eva', 28, 'eva@example.com'),
#     (6, 'Frank', 33, 'frank@example.com'),
#     (7, 'Grace', 45, 'grace@example.com'),
#     (8, 'Hannah', 22, 'hannah@example.com')
# """)

# spark.sql("SELECT * FROM spark_catalog.iceberg.my_table").show()

#3.time travel
# # 모든 테이블 내용 조회(time travel 이전)
# df = spark.sql("SELECT * FROM spark_catalog.iceberg.my_table order by id")
# df.show()

# # 스냅샷 확인
# spark.sql("SELECT * FROM spark_catalog.iceberg.my_table.snapshots").show(truncate=False)

# # time travel(이전 스냅샷으로 변경)
# # 특정 스냅샷으로 이동하여 데이터 조회
# snapshot_id = ''  # 조회한 스냅샷 ID 입력하세요
# df = spark.read.format("iceberg").option("snapshot-id", snapshot_id).load("spark_catalog.iceberg.my_table")
# df.show()



#4.partitioning
# spark.sql("""
# CREATE TABLE spark_catalog.iceberg.partitioned_table (
#     id INT,
#     name STRING,
#     age INT,
#     email STRING
# ) USING iceberg
# PARTITIONED BY (age)
# """)

# spark.sql("""
# INSERT INTO spark_catalog.iceberg.partitioned_table (id, name, age, email)
# VALUES 
#     (1, 'Alice', 30, 'alice@example.com'),
#     (2, 'Bob', 25, 'bob@example.com'),
#     (3, 'Charlie', 35, 'charlie@example.com'),
#     (4, 'David', 40, 'david@example.com'),
#     (5, 'Eva', 28, 'eva@example.com'),
#     (6, 'Frank', 30, 'frank@example.com'),
#     (7, 'Grace', 35, 'grace@example.com'),
#     (8, 'Hannah', 40, 'hannah@example.com'),
#     (9, 'Ivy', 25, 'ivy@example.com'),
#     (10, 'Jack', 28, 'jack@example.com'),
#     (11, 'Kate', 30, 'kate@example.com'),
#     (12, 'Leo', 35, 'leo@example.com'),
#     (13, 'Mia', 40, 'mia@example.com'),
#     (14, 'Nina', 25, 'nina@example.com'),
#     (15, 'Oscar', 28, 'oscar@example.com'),
#     (16, 'Paul', 30, 'paul@example.com'),
#     (17, 'Quinn', 35, 'quinn@example.com'),
#     (18, 'Rita', 40, 'rita@example.com'),
#     (19, 'Sam', 25, 'sam@example.com'),
#     (20, 'Tina', 28, 'tina@example.com'),
#     (21, 'Uma', 30, 'uma@example.com'),
#     (22, 'Vince', 35, 'vince@example.com'),
#     (23, 'Wendy', 40, 'wendy@example.com'),
#     (24, 'Xander', 25, 'xander@example.com'),
#     (25, 'Yara', 28, 'yara@example.com'),
#     (26, 'Zane', 30, 'zane@example.com')
# """)

# 5.아이스버그 테이블을 대상으로 쿼리 실행
# 1) Select All records
# df = spark.sql("SELECT * FROM spark_catalog.iceberg.partitioned_table")
# df.show()

# 2) Filter Records
# df = spark.sql("SELECT * FROM spark_catalog.iceberg.partitioned_table WHERE age = 30")
# df.show()

# # 3) Aggregate Data
# df = spark.sql("SELECT age, COUNT(*) as count FROM spark_catalog.iceberg.partitioned_table GROUP BY age")
# df.show()

spark.stop()
print("spark세션종료")