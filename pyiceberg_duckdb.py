import re
import duckdb
import psycopg2
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions.parser import parse
from pyiceberg.table import Table as IcebergTable
from urllib.parse import urlparse
from sqlglot import parse_one, exp

# PostgreSQL config
PG_CONFIG = {
    'dbname': 'benchbase',
    'user': 'postgres',
    'password': 'wnsdud318',
    'host': 'localhost',
    'port': 5432
}
PG_CONN_STRING = "host=localhost port=5432 dbname=benchbase user=postgres password=wnsdud318"

# Iceberg config
ICEBERG_CATALOG_NAME = "rest"
ICEBERG_WAREHOUSE = "s3://vldb-000/iceberg/"
catalog = load_catalog("rest", uri="http://localhost:8181", warehouse=ICEBERG_WAREHOUSE, type="rest")


def extract_table_names(query: str) -> list:
    tree = parse_one(query)
    return list({t.name for t in tree.find_all(exp.Table)})

def get_pg_schema(table: str):
    with psycopg2.connect(**PG_CONFIG) as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """, (table,))
        return cur.fetchall()

def duckdb_type(pg_type: str) -> str:
    mapping = {
        "smallint": "SMALLINT",
        "integer": "INTEGER",
        "bigint": "BIGINT",
        "real": "REAL",  # float4
        "double precision": "DOUBLE",  # float8
        "numeric": "DECIMAL(18,4)",  # 보수적 default precision/scale
        "decimal": "DECIMAL(18,4)",  # alias
        "money": "DECIMAL(18,2)",  # 보수적 처리

        "varchar": "VARCHAR",
        "character varying": "VARCHAR",
        "char": "VARCHAR",
        "character": "VARCHAR",
        "text": "TEXT",

        "boolean": "BOOLEAN",
        "bool": "BOOLEAN",

        "timestamp without time zone": "TIMESTAMP",
        "timestamp with time zone": "TIMESTAMPTZ",
        "date": "DATE",
        "time": "TIME",

        "uuid": "UUID",
        "bytea": "BLOB"
    }
    return mapping.get(pg_type.lower(), "VARCHAR")

def register_schema_only_table(conn: duckdb.DuckDBPyConnection, table: str, columns: list):
    col_defs = ", ".join(f"{name} {duckdb_type(dtype)}" for name, dtype in columns)
    conn.execute(f"CREATE OR REPLACE TABLE {table} ({col_defs})")


def extract_predicate_from_explain(conn: duckdb.DuckDBPyConnection, query: str, table_names: list) -> dict:
    for t in table_names:
        schema = get_pg_schema(t)
        register_schema_only_table(conn, t, schema)
    explain = conn.execute(f"EXPLAIN {query}").fetchall()
    predicate_map = {}
    for row in explain:
        match = re.search(r"Filter: (.+)", row[0])
        if match:
            predicate = match.group(1)
            for table in table_names:
                if table not in predicate_map:
                    predicate_map[table] = predicate
    return predicate_map

def get_filtered_parquet_paths(table_name: str, predicate: str) -> list[str]:
    try:
        table: IcebergTable = catalog.load_table(f"iceberg.{table_name}")
        expr = parse(predicate)
        task_files = table.scan(row_filter=expr).plan_files()

        def resolve_path(task):
            file = task.file
            path = getattr(file, "file_path", None) or getattr(file, "path", None)
            if not isinstance(path, str):
                raise TypeError(f"Expected string path in DataFile, got {type(path)}")
            if urlparse(path).scheme in {"s3", "s3a", "file"}:
                return path
            return f"{ICEBERG_WAREHOUSE.rstrip('/')}/{path}"

        return [resolve_path(task) for task in task_files]
    except Exception as e:
        return []

def rewrite_tables_with_union(query: str, available_iceberg_tables: list[str]) -> str:
    tree = parse_one(query)

    for table in tree.find_all(exp.Table):
        original_name = table.name
        alias = table.alias or exp.to_identifier(original_name)

        if original_name in available_iceberg_tables:
            union_sql = f"SELECT * FROM {original_name}_pg UNION ALL SELECT * FROM {original_name}_ice"
        else:
            union_sql = f"SELECT * FROM {original_name}_pg"

        subquery = parse_one(union_sql)
        table.replace(exp.Subquery(this=subquery, alias=alias))

    return tree.sql()


def setup_duckdb_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL aws; LOAD aws;")
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("SET s3_region='ap-northeast-2';")
    conn.execute("SET s3_access_key_id='DUMMY_ACCESS_KEY_ID';")
    conn.execute("SET s3_secret_access_key='DUMMY_SECRET_KEY_VALUE';")
    conn.execute("SET s3_endpoint='s3.ap-northeast-2.amazonaws.com';")
    return conn

def execute_query(query: str) -> duckdb.DuckDBPyRelation:
    conn = setup_duckdb_connection()
    table_names = extract_table_names(query)
    predicate_map = extract_predicate_from_explain(conn, query, table_names)

    # Iceberg view가 실제로 생성된 테이블만 rewrite 대상으로 삼기
    available_iceberg_tables = set()

    for table in table_names:
        # DuckDB VIEW → PostgreSQL
        conn.execute(f"""
            CREATE OR REPLACE VIEW {table}_pg AS
            SELECT * FROM postgres_scan('{PG_CONN_STRING}', 'public', '{table}');
        """)
        # DuckDB VIEW → Iceberg (Parquet 경로 기반)
        predicate = predicate_map.get(table, "True")
        parquet_paths = get_filtered_parquet_paths(table, predicate)
        if parquet_paths:
            parquet_str = ", ".join(f"'{p}'" for p in parquet_paths)
            conn.execute(f"""
                CREATE OR REPLACE VIEW {table}_ice AS
                SELECT * FROM read_parquet([{parquet_str}]);
            """)
            available_iceberg_tables.add(table)
        else:
            pass

    # rewrite 시점에 이 집합만 넘기기
    final_query = rewrite_tables_with_union(query, available_iceberg_tables)
    print(final_query)

    return conn.execute(final_query).fetchdf()

# CLI
if __name__ == "__main__":
    while True:
        query = input("💬 Enter SQL query (or 'exit'): ").strip()
        if query.lower() == "exit":
            break
        try:
            result = execute_query(query)
            print(result)
        except Exception as e:
            print("❌ Error:", e)
