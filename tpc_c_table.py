import os
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

# JAR 파일 경로를 설정
hadoop_aws_jar = "/Users/vldb/Downloads/석사학위논문/hadoop-aws-3.3.4.jar"
aws_sdk_jar = "/Users/vldb/Downloads/석사학위논문/bundle-2.29.38.jar"
iceberg_jar = "/Users/vldb/Downloads/석사학위논문/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
table_bucket_jar = "/Users/vldb/Downloads/석사학위논문/s3-tables-catalog-for-iceberg-0.1.3.jar"
commons_config_jar = "/Users/vldb/Downloads/석사학위논문/commons-configuration2-2.11.0.jar"
caffeine_jar = "/Users/vldb/Downloads/석사학위논문/caffeine-3.1.8.jar"
aws_bundle_jar = "/Users/vldb/Downloads/석사학위논문/aws-java-sdk-bundle-1.12.661.jar"

# S3 Table Bucket ARN 설정
TABLE_BUCKET_ARN = "arn:aws:s3tables:us-east-1:816069140297:bucket/vldb-001-table"

# SparkConf 설정
conf = SparkConf()
conf.set("spark.jars", f"{aws_bundle_jar},{caffeine_jar},{commons_config_jar},{hadoop_aws_jar},{aws_sdk_jar},{iceberg_jar},{table_bucket_jar}")
conf.set("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog")
conf.set("spark.sql.catalog.s3tablesbucket.warehouse", TABLE_BUCKET_ARN)

# 환경 변수에서 AWS 키를 읽도록 설정 (보안 강화)
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "your-access-key")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "your-secret-key")
conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key)
conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
conf.set("spark.hadoop.fs.s3a.endpoint", "s3tables.us-east-1.amazonaws.com")
conf.set("spark.hadoop.fs.s3a.aws.region", "us-east-1")
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

# SparkSession 생성
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# # namespace 생성
print("Creating Namespace 'tpc_c'...")
spark.sql("""
CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.tpcc
""")
print("Namespace 'tpcc' created successfully.")

# TPC-C 테이블 스키마 정의
tables = {
    "customer": """
        c_id INT,                         -- 고객 ID
        c_d_id INT,                       -- 지역 ID
        c_w_id INT,                       -- 창고 ID
        c_first STRING,                   -- 이름
        c_middle STRING,                  -- 중간 이름
        c_last STRING,                    -- 성
        c_address STRING,                 -- 주소
        c_phone STRING,                   -- 전화번호
        c_credit STRING,                  -- 신용 상태 ('GC' 또는 'BC')
        c_credit_lim DECIMAL(12,2),       -- 신용 한도
        c_discount DECIMAL(4,4),          -- 할인율
        c_balance DECIMAL(12,2),          -- 잔고
        c_ytd_payment DECIMAL(12,2),      -- 연간 결제
        c_payment_cnt INT,                -- 결제 횟수
        c_delivery_cnt INT,               -- 배송 횟수
        c_comment STRING                  -- 고객 정보
    """,
    "district": """
        d_id INT,                         -- 지역 ID
        d_w_id INT,                       -- 창고 ID
        d_name STRING,                    -- 지역 이름
        d_street_1 STRING,                -- 주소 1
        d_street_2 STRING,                -- 주소 2
        d_city STRING,                    -- 도시
        d_state STRING,                   -- 주
        d_zip STRING,                     -- 우편번호
        d_tax DECIMAL(4,4),               -- 세율
        d_ytd DECIMAL(12,2),              -- 연간 매출
        d_next_o_id INT                   -- 다음 주문 ID
    """,
    "hhistory": """
        h_c_id INT,                       -- 고객 ID
        h_c_d_id INT,                     -- 지역 ID
        h_c_w_id INT,                     -- 창고 ID
        h_d_id INT,                       -- 지역 ID
        h_w_id INT,                       -- 창고 ID
        h_date TIMESTAMP,                 -- 결제 날짜
        h_amount DECIMAL(12,2),           -- 결제 금액
        h_data STRING                     -- 추가 데이터
    """,
    "item": """
        i_id INT,                         -- 상품 ID
        i_im_id INT,                      -- 이미지 ID
        i_name STRING,                    -- 상품 이름
        i_price DECIMAL(12,2),            -- 상품 가격
        i_data STRING                     -- 추가 데이터
    """,
    "new_order": """
        no_o_id INT,                      -- 주문 ID
        no_d_id INT,                      -- 지역 ID
        no_w_id INT                       -- 창고 ID
    """,
    "oorder": """
        o_id INT,                         -- 주문 ID
        o_c_id INT,                       -- 고객 ID
        o_d_id INT,                       -- 지역 ID
        o_w_id INT,                       -- 창고 ID
        o_entry_d TIMESTAMP,              -- 주문 날짜
        o_carrier_id INT,                 -- 배송 업체 ID
        o_ol_cnt INT,                     -- 주문 항목 수
        o_all_local BOOLEAN               -- 모든 항목이 로컬 여부
    """,
    "order_line": """
        ol_o_id INT,                      -- 주문 ID
        ol_d_id INT,                      -- 지역 ID
        ol_w_id INT,                      -- 창고 ID
        ol_number INT,                    -- 주문 항목 번호
        ol_i_id INT,                      -- 상품 ID
        ol_supply_w_id INT,               -- 공급 창고 ID
        ol_delivery_d TIMESTAMP,          -- 배송 날짜
        ol_quantity DECIMAL(4,2),         -- 수량
        ol_amount DECIMAL(12,2),          -- 금액
        ol_dist_info STRING               -- 배포 정보
    """,
    "stock": """
        s_i_id INT,                       -- 상품 ID
        s_w_id INT,                       -- 창고 ID
        s_quantity INT,                   -- 재고 수량
        s_ytd INT,                        -- 연간 주문 수량
        s_order_cnt INT,                  -- 주문 횟수
        s_remote_cnt INT,                 -- 원격 주문 횟수
        s_data STRING                     -- 추가 데이터
    """,
    "warehouse": """
        w_id INT,                         -- 창고 ID
        w_name STRING,                    -- 창고 이름
        w_street_1 STRING,                -- 주소 1
        w_street_2 STRING,                -- 주소 2
        w_city STRING,                    -- 도시
        w_state STRING,                   -- 주
        w_zip STRING,                     -- 우편번호
        w_tax DECIMAL(4,4),               -- 세율
        w_ytd DECIMAL(12,2)               -- 연간 매출
    """
}



# Iceberg 테이블 생성
for table_name, schema in tables.items():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS s3tablesbucket.tpcc.{table_name} (
            {schema}
        ) USING iceberg
    """)

print("All TPC-C tables have been successfully created in the S3 Table Bucket.")