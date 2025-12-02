#!/usr/bin/env python3
"""
간단한 벤치마크 러너:
- postgres-naive: OLAP 쿼리를 Postgres에서 실행
- p2i: OLAP 쿼리를 DuckDB(또는 Spark)로 Iceberg에서 읽어 실행
결과를 JSON으로 저장합니다.
"""
import subprocess, time, json, os
from datetime import datetime

RESULT_DIR = os.getenv("RESULT_DIR", "./results")
os.makedirs(RESULT_DIR, exist_ok=True)
POSTGRES_CONN = os.getenv("PG_CONN", "postgresql://postgres:postgres@localhost:5432/postgres")
DUCKDB_CMD = os.getenv("DUCKDB_CMD", "duckdb")

OLAP_QUERIES = [
    ("q_total_amount_by_customer", "SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id"),
    ("q_top_customers", "SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id ORDER BY total DESC LIMIT 10"),
]

def run_postgres_query(sql):
    p = subprocess.run(["psql", POSTGRES_CONN, "-c", sql], capture_output=True, text=True)
    return p

def run_duckdb_query_iceberg(query):
    temp_sql = f"""
    INSTALL httpfs;
    LOAD httpfs;
    {query}
    """
    p = subprocess.run([DUCKDB_CMD, "-c", temp_sql], capture_output=True, text=True)
    return p

def measure_query(label, runner, *args):
    t0 = time.time()
    r = runner(*args)
    t1 = time.time()
    duration = t1 - t0
    return {"label": label, "duration": duration, "returncode": r.returncode, "stdout": r.stdout, "stderr": r.stderr}

def main():
    results = {"meta": {"start": str(datetime.utcnow())}, "runs": []}
    for name, sql in OLAP_QUERIES:
        r = measure_query(f"pg_{name}", run_postgres_query, sql)
        results["runs"].append(r)
    for name, sql in OLAP_QUERIES:
        r = measure_query(f"p2i_{name}", run_duckdb_query_iceberg, sql)
        results["runs"].append(r)
    out_json = os.path.join(RESULT_DIR, f"bench_{int(time.time())}.json")
    with open(out_json, "w") as f:
        json.dump(results, f, indent=2)
    print("bench results saved to", out_json)

if __name__ == "__main__":
    main()