import psycopg2
import duckdb
import pandas as pd
import re
import requests
from pyspark.sql import SparkSession
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions.parser import parse
from pyiceberg.table import Table as IcebergTable
import datetime
import sqlglot
from sqlglot import exp, parse_one

# JAR 파일 경로를 설정 (코드와 동일한 디렉토리에 JAR 파일이 있음)
hadoop_aws_jar = "/home/vldb/jun/p2i/hadoop-aws-3.3.4.jar"
aws_sdk_jar = "/home/vldb/jun/p2i/aws-java-sdk-bundle-1.12.661.jar"
iceberg_jar = "/home/vldb/jun/p2i/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
bundle_jar="/home/vldb/jun/p2i/bundle-2.29.38.jar"
commons_jar="/home/vldb/jun/p2i/commons-configuration2-2.11.0.jar"
postgresql_jar = "/home/vldb/jun/p2i/postgresql-42.7.1.jar"


# PostgreSQL 실행 관련 변수
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "benchbase"

# AWS S3 인증 정보
AWS_REGION = "ap-northeast-2"
AWS_ACCESS_KEY = "DUMMY_ACCESS_KEY_ID"
AWS_SECRET_KEY = "DUMMY_SECRET_KEY_VALUE"

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

# FK로 조회한 자식 테이블의 스키마, 테이블명 분리
def parse_schema_and_table(full_name: str):
    parts = full_name.split(".")
    if len(parts) == 2:
        return parts[0], parts[1]
    else:
        return "public", full_name

def drop_fk_constraints(table_name):
    """
    주어진 테이블을 참조하는 FK constraint를 전부 찾아서 Drop하는 함수
    """
    conn = psycopg2.connect(
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )
    cur = conn.cursor()

    get_fk_sql = """
    SELECT conname, conrelid::regclass::text
    FROM pg_constraint
    WHERE contype = 'f'
      AND confrelid = %s::regclass;
    """
    cur.execute(get_fk_sql, (table_name,))
    fks = cur.fetchall()

    for conname, child_table in fks:
        drop_sql = f"ALTER TABLE {child_table} DROP CONSTRAINT {conname};"
        print(f"👉 Dropping FK: {drop_sql}")
        cur.execute(drop_sql)

    conn.commit()
    cur.close()
    conn.close()

def run_spark_pipeline_simple(query, table_name, spark):
    # 부모 테이블 데이터 읽기
    spark_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"({query}) AS subquery") \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # Iceberg에 append
    iceberg_table = f"rest.iceberg.{table_name.replace('.', '_')}"
    if not spark._jsparkSession.catalog().tableExists(iceberg_table):
        columns = get_postgres_schema(table_name)
        create_sql = generate_iceberg_create_table(table_name, columns)
        spark.sql(create_sql)

    spark_df.writeTo(iceberg_table).append()

    # 부모 테이블 삭제
    key_columns = get_primary_key_columns(table_name)
    staging_table = f"staging_delete_{table_name.replace('.', '_')}"
    spark_df.select(*key_columns).distinct().write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", staging_table) \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    # DELETE 수행 (staging table 기반)
    delete_sql = f"DELETE FROM {table_name} USING {staging_table} WHERE " + \
                 " AND ".join([f"{table_name}.{col} = {staging_table}.{col}" for col in key_columns])

    conn = psycopg2.connect(
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )
    cur = conn.cursor()
    cur.execute(delete_sql)
    conn.commit()
    cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
    conn.commit()
    cur.close()
    conn.close()

# spark를 통해 iceberg metadata aware하게 데이터를 fetch해온 다음 arrow로 duckdb에 전달
def fetch_iceberg_filtered_arrow(table_name: str, predicate: str, spark: SparkSession):
    """
    Iceberg에서 predicate 기반으로 필요한 데이터만 읽고
    Pandas/Arrow로 변환하여 DuckDB로 넘기기 위한 함수
    """
    full_table = f"{ICEBERG_NAMESPACE}.{table_name}"

    query = f"""
        SELECT * FROM {full_table}
        WHERE {predicate}
    """

    print(f"🔥 Executing Iceberg filter query on Spark: {query}")
    df = spark.sql(query)

    # return either:
    # return df.toPandas()
    return df.to_arrow_table()



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


def extract_table_names(query: str) -> set:
    """
    sqlglot 기반으로 쿼리 내 모든 FROM/JOIN 대상 테이블 이름을 추출
    """
    table_names = set()
    tree = sqlglot.parse_one(query)
    for table_expr in tree.find_all(exp.Table):
        table_names.add(table_expr.name)
    return table_names



# def convert_query_to_iceberg(query: str, pg_used: bool) -> str:
#     """
#     Iceberg-aware SQL 변환기 (predicate pushdown 지원):
#     - Iceberg 테이블은 iceberg_scan()으로 wrapping
#     - WHERE 절이 존재하면 iceberg_scan 내부로 pushdown
#     - alias는 i_{table}
#     - ORDER BY, GROUP BY, LIMIT 등은 UNION ALL 외부로 뺌
#     """
#     tree = sqlglot.parse_one(query)

#     # 1. tail clause 추출 (ORDER BY, GROUP BY, etc.)
#     tail_clauses = []
#     for clause_type in ("order", "group", "having", "limit", "offset"):
#         clause = tree.args.pop(clause_type, None)
#         if clause:
#             tail_clauses.append(clause)

#     # 2. WHERE 절 분리 (전체 WHERE)
#     where_clause = tree.args.pop("where", None)

#     # 3. FROM/JOIN 테이블 → iceberg_scan 서브쿼리
#     for table_expr in tree.find_all(exp.Table):
#         table_name = table_expr.name
#         alias = f"i_{table_name}"

#         try:
#             url = f"http://0.0.0.0:8181/v1/namespaces/iceberg/tables/{table_name}"
#             metadata_location = requests.get(url).json()["metadata-location"]

#             # 3-1. 필터 조건 추출 (WHERE이 존재할 경우)
#             if where_clause:
#                 # predicate 문자열 추출
#                 condition_expr=where_clause.args["this"]
#                 condition_sql = condition_expr.sql(dialect="duckdb")

#                 scan_with_filter = f"""
#                 SELECT * FROM iceberg_scan('{metadata_location}')
#                 WHERE {condition_sql}
#                 """
#             else:
#                 scan_with_filter = f"SELECT * FROM iceberg_scan('{metadata_location}')"

#             iceberg_subquery = exp.Subquery(
#                 this=sqlglot.parse_one(scan_with_filter),
#                 alias=exp.TableAlias(this=exp.to_identifier(alias))
#             )

#             table_expr.replace(iceberg_subquery)

#         except Exception as e:
#             print(f"⚠️ Iceberg 변환 실패 - {table_name}: {e}")
#             continue

#     # 4. 변환된 쿼리 → 문자열
#     main_sql = tree.sql()
#     tail_sql = " ".join([c.sql() for c in tail_clauses])

#     # 5. 최종 구성
#     if pg_used:
#         final_sql = f"""
#         SELECT * FROM (
#             {main_sql}
#             UNION ALL
#             SELECT * FROM postgres.pg_result
#         ) AS p_{table_name}
#         {tail_sql}
#         """
#     else:
#         final_sql = f"{main_sql} {tail_sql}"

#     return final_sql.strip()+";"

def extract_conditions(where_expr):
    """
    WHERE 절에서 테이블 단독 조건 vs JOIN 조건 분리
    """
    table_filters = {}
    join_conditions = []

    def helper(expr):
        if isinstance(expr, exp.And):
            return helper(expr.left) + helper(expr.right)
        return [expr]

    conditions = helper(where_expr) if where_expr else []

    for cond in conditions:
        involved_tables = {t.name for t in cond.find_all(exp.Column) if t.table}
        if len(involved_tables) == 1:
            table = next(iter(involved_tables))
            table_filters.setdefault(table, []).append(cond)
        else:
            join_conditions.append(cond)

    return table_filters, join_conditions


def is_single_table_simple_select(tree: exp.Expression) -> bool:
    return (
        isinstance(tree, exp.Select)
        and len(list(tree.find_all(exp.Table))) == 1
        and not tree.args.get("joins")
    )


def convert_query_to_iceberg(query: str, pg_used: bool) -> str:
    """
    Iceberg-aware SQL 변환기 (JOIN-aware + projection-aware):
    - Iceberg 테이블은 iceberg_scan()으로 wrapping
    - WHERE 절이 존재하면 iceberg_scan 내부로 pushdown
    - alias는 i_{table}
    - ORDER BY, GROUP BY, LIMIT 등은 UNION ALL 외부로 뺌
    - 단일 테이블이면 SELECT 감싸지 않음
    """
    tree = parse_one(query)

    # 1. tail clause 추출 (ORDER BY, GROUP BY, LIMIT, etc.)
    tail_clauses = []
    for clause_type in ("order", "group", "having", "limit", "offset"):
        clause = tree.args.pop(clause_type, None)
        if clause:
            tail_clauses.append(clause)

    # 2. WHERE 절 분리
    where_clause = tree.args.pop("where", None)
    table_filters, join_conditions = extract_conditions(where_clause)

    # 3. Iceberg wrapping (FROM/JOIN 대상 테이블)
    for table_expr in tree.find_all(exp.Table):
        table_name = table_expr.name
        alias = f"i_{table_name}"

        try:
            url = f"http://0.0.0.0:8181/v1/namespaces/iceberg/tables/{table_name}"
            metadata_location = requests.get(url).json()["metadata-location"]

            filters = table_filters.get(table_name, [])
            if filters:
                where_sql = " AND ".join([f.sql(dialect="duckdb") for f in filters])
                scan_sql = f"SELECT * FROM iceberg_scan('{metadata_location}') WHERE {where_sql}"
            else:
                scan_sql = f"SELECT * FROM iceberg_scan('{metadata_location}')"

            subquery = exp.Subquery(
                this=parse_one(scan_sql),
                alias=exp.TableAlias(this=exp.to_identifier(alias))
            )
            table_expr.replace(subquery)

        except Exception as e:
            print(f"⚠️ Iceberg 변환 실패 - {table_name}: {e}")
            continue

    # 4. WHERE 절 다시 추가 (JOIN 조건만)
    if join_conditions:
        tree.set("where", exp.and_(*join_conditions))

    # 5. tail 절 복원
    tail_sql = " ".join([c.sql() for c in tail_clauses])

    # 6. DuckDB + Postgres UNION 처리
    if pg_used:
        if is_single_table_simple_select(tree):
            # projection 직접 추출
            projection_cols = ", ".join([p.sql() for p in tree.expressions])
            pg_query = f"SELECT {projection_cols} FROM postgres.public.pg_result"

            final_sql = f"""
            {tree.sql()}
            UNION ALL
            {pg_query}
            {tail_sql}
            """
        else:
            main_sql = tree.sql()
            final_sql = f"""
            SELECT * FROM (
                {main_sql}
                UNION ALL
                SELECT * FROM postgres.public.pg_result
            ) AS p_final
            {tail_sql}
            """
    else:
        final_sql = f"{tree.sql()} {tail_sql}"

    return final_sql.strip() + ";"




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

#######################################################


import psycopg2


def format_query_with_args(query: str, args: list) -> str:
    """
    SELECT 쿼리에서 CREATE VIEW를 만들기 위해 SQL-safe하게 args를 직접 치환
    ※ 반드시 ? → %s 변환 이후 호출할 것
    """
    formatted = []
    for a in args:
        if isinstance(a, str):
            formatted.append("'" + a.replace("'", "''") + "'")
        elif isinstance(a, datetime.datetime):
            formatted.append("'" + a.isoformat(sep=" ") + "'")
        elif a is None:
            formatted.append("NULL")
        else:
            formatted.append(str(a))

    if query.count("%s") != len(formatted):
        raise ValueError(
            f"❌ 포맷팅 실패: 쿼리 내 %s 개수({query.count('%s')})와 인자 개수({len(formatted)}) 불일치\n쿼리: {query}\nargs: {formatted}"
        )

    return query % tuple(formatted)


def sanitize_pg_args(args: list) -> list:
    """
    psycopg2 바인딩용 인자 변환: datetime → isoformat 문자열
    """
    result = []
    for a in args:
        if isinstance(a, datetime.datetime):
            result.append(a.isoformat(sep=" "))
        else:
            result.append(a)
    return result


def execute_postgres_query_with_args(query, args):
    try:
        conn = psycopg2.connect(
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        cur = conn.cursor()

        query = query.strip()
        is_select = query.upper().startswith("SELECT")

        if is_select:
            # ✅ SELECT → CREATE VIEW pg_result
            cur.execute("DROP VIEW IF EXISTS pg_result;")

            query = query.replace("?", "%s")
            formatted_query = format_query_with_args(query, args)

            if "FOR UPDATE" in formatted_query.upper():
                cur.execute(formatted_query)
                result = cur.fetchall()
                conn.commit()
                print("✅ PostgreSQL: 트랜잭션 락 생성 성공")
                return result

            create_view_query = f"CREATE VIEW pg_result AS {formatted_query}"

            cur.execute(create_view_query)
            conn.commit()
            print("✅ PostgreSQL: pg_result 뷰 생성 성공")
            return True  # SELECT → 뷰 생성 성공

        else:
            # ✅ INSERT / UPDATE / DELETE 등은 바인딩 처리
            query = query.replace("?", "%s")
            args = sanitize_pg_args(args)

            cur.execute(query, args)
            conn.commit()
            print("✅ PostgreSQL 쿼리 실행 성공")
            return None

    except psycopg2.Error as e:
        print("❌ PostgreSQL Error:", e)
        return None

    finally:
        cur.close()
        conn.close()


def execute_duckdb_olap_with_args(query, args):
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
        pg_flag = execute_postgres_query_with_args(query, args)


        # ✅ 사용자의 쿼리를 Iceberg 테이블 대상으로 변환
        converted_query = convert_query_to_iceberg(query)

        print(converted_query)

        # view가 생성되었을 경우, union all query로 변경
        if pg_flag is True:
            # ✅ DuckDB에서 PostgreSQL 데이터를 pg_result로 조인 (customer 아님), attach할 때 as postgres로 설정했으므로 postgres.pg_result
            converted_query = converted_query.strip().rstrip(";")
            converted_query += " UNION ALL Select * from postgres.pg_result;"
            print("join 추가:", converted_query)

        # ✅ DuckDB에서 쿼리 실행
        iceberg_df = conn.execute(converted_query, args).fetchdf()
        return iceberg_df


    except Exception as e:
        print("❌ DuckDB Error with args:", e)
        return None
    
  

        #OLAP에 데이터가 더 많다는 가정



def execute_query_with_args(query, args):
    query_type = classify_query(query)
    print(f"🔍 Query Type: {query_type} | Args: {args}")

    if query_type == "OLTP":
        return execute_postgres_query_with_args(query, args)

    elif query_type == "OLAP":
        if "FOR UPDATE" in query.upper():
            return execute_postgres_query_with_args(query, args)
        else:
            return execute_duckdb_olap_with_args(query, args)

    else:
        print("❌ Unknown query type.")
        return None


#######################################
# 뷰 생성할 때 기존의 세션에서 열면 미커밋된 내용을 반영할 수 있음 -> 하지만, 덕디비에서 조회하기 위해서는 뷰를 커밋해야하는 문제가 발생함
# 그래서 아예 세션을 새로 열어서 생성하였음.
# 미커밋된 내용을 보지는 못하지만, 우선 그래도 괜찮다고 가정하고 구현하였음
def create_pg_view_isolated(formatted_view_query: str) -> bool:
    """
    PostgreSQL에서 formatted SELECT 쿼리를 기반으로 pg_result 뷰를 생성하고
    row count를 반환하는 작업을 autocommit 커넥션에서 처리한다.
    """
    try:
        view_conn = psycopg2.connect(
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        view_conn.autocommit = True
        view_cur = view_conn.cursor()

        view_cur.execute("DROP VIEW IF EXISTS public.pg_result;")
        view_cur.execute(f"CREATE VIEW public.pg_result AS {formatted_view_query};")
        print("✅ (격리) VIEW 생성 성공")

        view_cur.execute("SELECT COUNT(*) FROM public.pg_result;")
        row_count = view_cur.fetchone()[0]
        print("📈 (격리) ROW COUNT:", row_count)

        return row_count > 0

    except Exception as e:
        print("❌ (격리) VIEW 생성 오류:", e)
        raise

    finally:
        try:
            view_cur.close()
            view_conn.close()
            print("(격리) 연결 종료")
        except Exception as close_err:
            print("⚠️ (격리) 연결 종료 오류:", close_err)




#######################################
def execute_query_transactionally_atomic(statements: list):
    """
    하나의 DuckDB + 하나의 PostgreSQL 트랜잭션 세션 내에서 쿼리 리스트를 실행함.
    모든 쿼리가 성공해야 commit. 하나라도 실패 시 rollback.
    """
    results = []

    # 연결 열기
    conn_pg = psycopg2.connect(
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )
    conn_pg.autocommit=False
    cur_pg = conn_pg.cursor()

    cur_pg.execute("SET idle_in_transaction_session_timeout = '10s'")

    conn_duck = duckdb.connect()
    conn_duck.execute("INSTALL httpfs; LOAD httpfs;")
    conn_duck.execute("INSTALL aws; LOAD aws;")
    conn_duck.execute("INSTALL iceberg; LOAD iceberg;")
    conn_duck.execute("INSTALL postgres_scanner; LOAD postgres_scanner;")
    conn_duck.execute(f"SET s3_region='{AWS_REGION}';")
    conn_duck.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY}';")
    conn_duck.execute(f"SET s3_secret_access_key='{AWS_SECRET_KEY}';")
    conn_duck.execute(f"""
                    ATTACH 'dbname={PG_DATABASE} user={PG_USER} host={PG_HOST} password={PG_PASSWORD} port={PG_PORT}'
                    AS postgres (TYPE POSTGRES);
                """)

    try:
        
        for i, (sql, args) in enumerate(statements):
            cur_pg.execute("SELECT pg_backend_pid();")
            pid = cur_pg.fetchone()[0]
            print(f"🐘 PostgreSQL 세션 PID: {pid}")

            query_type = classify_query(sql)
            print(f"\n🧾 [{i+1}] {query_type}: {sql} | Args: {args}")

            if query_type == "OLTP":
                try:
                    sql_prepared = sql.replace("?", "%s")
                    print(f"\n🟦 [{i+1}] OLTP 쿼리 실행 시작:\n{sql} | Args: {args}")

                    cur_pg.execute(sql_prepared, sanitize_pg_args(args))
                    print(f"✅ [{i+1}] 쿼리 실행 성공")

                    if sql.strip().upper().startswith("SELECT"):
                        rows = cur_pg.fetchall()
                        print(f"🔎 [{i+1}] SELECT 결과: {len(rows)} rows")
                        results.append({"index": i, "type": "select", "rows": len(rows)})
                    else:
                        affected = cur_pg.rowcount
                        print(f"✏️ [{i+1}] WRITE 결과: {affected} rows affected")
                        results.append({"index": i, "type": "write", "affected": affected})

                except Exception as e:
                    print(f"❌ [{i+1}] OLTP 쿼리 실행 중 예외 발생:\n{sql} | Args: {args}\n→ 에러: {e}")
                    raise


            elif query_type == "OLAP":
                sql_prepared = sql.replace("?", "%s")
                formatted = format_query_with_args(sql_prepared, args)
                print("📌 formatted query:\n", formatted)

                if "FOR UPDATE" in formatted.upper():
                    print("🔒 FOR UPDATE 감지됨, 트랜잭션 락 쿼리 실행 중...")
                    cur_pg.execute(formatted)
                    result = cur_pg.fetchall()
                    print("✅ FOR UPDATE 쿼리 실행 결과:", result)

                    # 이 쿼리는 DuckDB로 안 넘길 거니까 continue
                    continue

                try:
                    pg_used = create_pg_view_isolated(formatted)
                except Exception as e:
                    print("❌ PostgreSQL 뷰 생성 중 오류 발생:", e)
                    raise

                conn_duck.execute("CALL pg_clear_cache()")

                converted = convert_query_to_iceberg(sql, pg_used).strip()
                print("🐤 DuckDB 최종 실행 쿼리:\n", converted)

                try:
                    print(f"🐤 DuckDB 실행 시작: {converted}")
                    df = conn_duck.execute(converted, args).fetchdf()
                    print(f"✅ DuckDB 실행 성공, row count: {len(df)}")
                    results.append({"index": i, "type": "olap", "rows": len(df)})
                except Exception as e:
                    print("❌ DuckDB 실행 중 오류 발생 → PostgreSQL 트랜잭션 롤백")
                    try:
                        conn_pg.rollback()
                        print("🔁 PostgreSQL rollback 완료")
                    except Exception as rb_err:
                        print("⚠️ rollback 중 오류:", rb_err)
                    raise

            else:
                raise ValueError(f"❌ Unknown query type at index {i}: {sql}")

        # ✅ 전체 성공했을 때만 commit
        conn_pg.commit()

        print("\n📦 전체 트랜잭션 결과 요약:")
        for r in results:
            if r["type"] == "select":
                print(f"🔎 Query[{r['index']}] SELECT → {r['rows']} rows")
            elif r["type"] == "write":
                print(f"✏️ Query[{r['index']}] WRITE → {r['affected']} rows affected")
            elif r["type"] == "olap":
                print(f"📊 Query[{r['index']}] OLAP → {r['rows']} rows")

        return {"status": "ok", "executed": len(results), "results": results}

    except Exception as e:
        # 실패 시 PostgreSQL 트랜잭션 롤백
        try:
            conn_pg.rollback()
        except Exception as rb_err:
            print("⚠️ 롤백 중 예외 발생:", rb_err)

        print("❌ 트랜잭션 실패 → 롤백:", e)
        return {
            "status": "error",
            "message": str(e),
            "executed": len(results),
            "results": results
        }

    finally:
        # 안전하게 닫기
        try:
            cur_pg.close()
            conn_pg.close()
        except Exception as close_err:
            print("⚠️ PostgreSQL 연결 종료 중 오류:", close_err)

        try:
            conn_duck.close()
        except Exception as close_err:
            print("⚠️ DuckDB 연결 종료 중 오류:", close_err)


# 실행
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("S3AccessExample") \
        .config("spark.jars",
                f"{postgresql_jar},{hadoop_aws_jar},{aws_sdk_jar},{iceberg_jar}, {bundle_jar}, {commons_jar}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", "https://s3.ap-northeast-2.amazonaws.com") \
        .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.rest.type", "rest") \
        .config("spark.sql.catalog.rest.uri", "http://0.0.0.0:8181") \
        .config("spark.sql.catalog.rest.warehouse", "s3a://vldb-000/iceberg/") \
        .getOrCreate()

    while True:
        user_query = input("💬 Enter your SQL query (or type 'exit' to quit): ").strip()
        if user_query.lower() == "exit":
            break

        table_names = extract_table_names(user_query)
        table_name = sorted(table_names)[0]

        drop_fk_constraints(table_name)  # FK 먼저 삭제
        run_spark_pipeline_simple(user_query, table_name, spark)

        result_df = execute_query(user_query)
        if result_df is not None:
            print(result_df)
