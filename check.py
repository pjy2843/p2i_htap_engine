from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

# Spark 세션 설정
spark = SparkSession.builder \
    .appName("Iceberg CDC Example") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3://vldb-000") \
    .config("spark.hadoop.fs.s3a.access.key", "DUMMY_ACCESS_KEY_ID") \
    .config("spark.hadoop.fs.s3a.secret.key", "DUMMY_SECRET_KEY_VALUE") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
    .getOrCreate()

# Kafka에서 CDC 데이터 확인용 코드 추가
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-cluster:9092") \
    .option("subscribe", "postgres.public.car_sales") \
    .load()

# Kafka 데이터의 스키마 및 샘플 데이터 출력
kafka_df.printSchema()
kafka_df.selectExpr("CAST(value AS STRING)").show(5, truncate=False)
