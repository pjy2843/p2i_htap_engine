import duckdb
import boto3

# DuckDB에서 httpfs 확장 로드
con = duckdb.connect()
con.execute("INSTALL aws")
con.execute("LOAD aws")
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")
con.execute("INSTALL iceberg")
con.execute("LOAD iceberg")


# S3 자격 증명 설정
con.execute("""
SET s3_region='us-east-1';
SET s3_access_key_id='DUMMY_ACCESS_KEY_ID';
SET s3_secret_access_key='DUMMY_SECRET_KEY_VALUE';
""")

import subprocess
import json

# AWS CLI 명령어 정의
command = [
    "aws", "s3tables", "get-table",
    "--table-bucket-arn", "arn:aws:s3tables:us-east-1:816069140297:bucket/vldb-000-table",
    "--namespace", "sample_namespace",
    "--name", "my_sample_table"
]

try:
    # 명령어 실행 및 결과 가져오기
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    
    # JSON 문자열을 Python 딕셔너리로 변환
    table_info = json.loads(result.stdout)
    
    # metadataLocation 값 추출
    
    metadata_location = table_info.get("metadataLocation", None)
    if metadata_location:
        print(f"Metadata Location: {metadata_location}")
    else:
        print("metadataLocation key not found in the result.")

except subprocess.CalledProcessError as e:
    # 에러 처리
    print(f"Error occurred: {e.stderr}")


# S3 table bucket에 저장된 아이스버그 테이블 읽기
query = f"SELECT * FROM iceberg_scan('{metadata_location}')"

# 쿼리 실행 및 결과 출력
df = con.execute(query).df()
print(df)
