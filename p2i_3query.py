import psycopg2
import duckdb
import pandas as pd
import re
import requests

# PostgreSQL 실행 관련 변수
PG_USER = "postgres"
PG_PASSWORD = "wnsdud318"
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "benchbase"

# AWS S3의 Iceberg 테이블 경로
S3_TABLE_PATH = "s3://vldb-000/iceberg/customer"

# AWS S3 인증 정보
AWS_REGION = "ap-northeast-2"
AWS_ACCESS_KEY = "DUMMY_ACCESS_KEY_ID"
AWS_SECRET_KEY = "DUMMY_SECRET_KEY_VALUE"

# OLTP/OLAP 쿼리 분류 함수
def classify_query(query):
    query = query.strip().upper()

    # OLTP 패턴 (INSERT, UPDATE, DELETE만 포함)
    oltp_patterns = [r"^\s*(INSERT|UPDATE|DELETE|REPLACE)\s"]

    # OLAP 패턴 (SELECT + 분석 연산 포함)
    olap_patterns = [
        r"\s(GROUP\sBY|HAVING|ORDER\sBY|DISTINCT|LIMIT)\s",
        r"\s(SUM|AVG|COUNT|MAX|MIN)\s*\(",
        r"\sJOIN\s"
    ]

    # OLTP 판별
    for pattern in oltp_patterns:
        if re.search(pattern, query):
            return "OLTP"

    # OLAP 판별
    for pattern in olap_patterns:
        if re.search(pattern, query):
            return "OLAP"

    # SELECT는 기본적으로 OLAP으로 처리
    if query.startswith("SELECT"):
        return "OLAP"

    return "UNKNOWN"


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

        # ✅ `FROM <table>` 또는 `JOIN <table>` 패턴을 찾아 Iceberg 서브쿼리로 변경 + `{table}` 별칭 추가
        pattern = rf"(?i)(FROM|JOIN|,)\s+{table}(\s|,|$)"
        replacement = rf"\1 (SELECT * FROM iceberg_scan('{metadata_location}')) AS {table}\2"
        query = re.sub(pattern, replacement, query)

    return query


### ✅ 2️⃣ DuckDB에서 Iceberg 쿼리 실행 & PostgreSQL 데이터와 병합
def execute_duckdb_olap(query):
    """DuckDB에서 Iceberg 테이블을 쿼리 실행"""
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

        #OLAP에 데이터가 더 많다는 가정

        conn.execute(f"""
            ATTACH 'dbname={PG_DATABASE} user={PG_USER} host={PG_HOST} password={PG_PASSWORD} port={PG_PORT}' AS postgres (TYPE POSTGRES);
        """)

        # view가 생성되었을 경우, pg_flag is not none, pg_result라는 이름의 뷰 생성
        pg_flag = execute_postgres_query(query)

        print(conn.execute("select * from postgres.pg_result"))

        # ✅ 사용자의 쿼리를 Iceberg 테이블 대상으로 변환
        converted_query = convert_query_to_iceberg(query)

        print(converted_query)

        #view가 생성되었을 경우, union all query로 변경
        if pg_flag is True:
            # ✅ DuckDB에서 PostgreSQL 데이터를 pg_result로 조인 (customer 아님), attach할 때 as postgres로 설정했으므로 postgres.pg_result
            converted_query = converted_query.strip().rstrip(";")
            converted_query += " UNION ALL Select * from postgres.pg_result;"
            print("join 추가:", converted_query)

        # ✅ DuckDB에서 쿼리 실행
        iceberg_df = conn.execute(converted_query).fetchdf()
        return iceberg_df

    except Exception as e:
        print("❌ DuckDB Error:", e)
        return None


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
    while True:
        user_query = input("💬 Enter your SQL query (or type 'exit' to quit): ").strip()
        if user_query.lower() == "exit":
            break

        result_df = execute_query(user_query)

        if result_df is not None:
            print(result_df)
