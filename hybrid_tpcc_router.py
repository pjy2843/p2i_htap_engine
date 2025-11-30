# ✅ hybrid_tpcc_router.py
# - BenchBase(TPCC)가 PostgreSQL에 날린 쿼리를 실시간 수집 (pg_stat_statements 기반)
# - 수집된 쿼리를 너의 하이브리드 쿼리 분석 파이프라인에 자동 전달

import psycopg2
import re
import time

# 하이브리드 쿼리 실행기
from p2i_postgres import execute_query_with_args  # 예: 이 함수는 query + args 받음

# PostgreSQL 연결 정보
PG_DB = "benchbase"
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_HOST = "localhost"
PG_PORT = "5432"
DSN = f"dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} host={PG_HOST} port={PG_PORT}"

seen_queries = set()

blacklist_patterns = [
    r"^SET ", r"^SHOW ", r"^BEGIN", r"^COMMIT", r"pg_stat_statements", r"pg_catalog"
]

def is_valid_query(q):
    q = q.strip().upper()
    return not any(re.match(p, q) for p in blacklist_patterns)

def replace_pg_placeholders(query):
    return re.sub(r"\$\d+", "?", query)

def count_pg_params(query):
    return len(re.findall(r"\$\d+", query))

def generate_dummy_args(n):
    return [1] * n  # 전부 1로 대체 (int), 필요 시 랜덤 or 실제 스키마 기반으로 변경 가능

def collect_tpcc_queries():
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT query
                FROM pg_stat_statements
                WHERE query ILIKE '%history%' OR query ILIKE '%order_line%' OR query ILIKE '%customer%'
                ORDER BY calls DESC
                LIMIT 50
            """)
            queries = [row[0].strip() for row in cur.fetchall() if is_valid_query(row[0])]
            return queries

def run_hybrid_replay_loop(interval_sec=10):
    print("🚀 Hybrid TPC-C query monitor 시작")
    while True:
        queries = collect_tpcc_queries()
        new_queries = [q for q in queries if q not in seen_queries]

        for pg_query in new_queries:
            seen_queries.add(pg_query)
            print("\n📥 수집된 원본 쿼리:", pg_query)

            try:
                param_count = count_pg_params(pg_query)
                dummy_args = generate_dummy_args(param_count)
                converted_query = replace_pg_placeholders(pg_query)
                print(f"🧪 변환된 쿼리: {converted_query} | args: {dummy_args}")

                execute_query_with_args(converted_query, dummy_args)

            except Exception as e:
                print("❌ 하이브리드 실행 실패:", e)

        time.sleep(interval_sec)

if __name__ == "__main__":
    run_hybrid_replay_loop()
