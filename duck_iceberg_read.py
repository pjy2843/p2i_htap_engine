import duckdb

# DuckDB 연결 및 확장 로드
con = duckdb.connect()
con.execute("INSTALL aws")
con.execute("LOAD aws")
con.execute("INSTALL iceberg")
con.execute("LOAD iceberg")

# S3 자격 증명 설정
con.execute("""
SET s3_region='ap-northeast-2';
SET s3_access_key_id='DUMMY_ACCESS_KEY_ID';
SET s3_secret_access_key='DUMMY_SECRET_KEY_VALUE';
""")


# Iceberg 테이블 읽기
try:
    result = con.execute("""
        SELECT * FROM read_parquet('s3a://vldb-000/iceberg/data/00001-1737521256890-ea939162-c2b8-4b8a-9f6b-4d8b6a94ff97-00003.parquet');
    """).fetchall()
    for row in result:
        print(row)
except Exception as e:
    print(f"Error reading Iceberg table: {e}")
