"""
def get_latest_snapshot_id(spark, catalog, namespace, table_name):
    # 시도 1: catalog의 system 테이블(예: rest.system.snapshots)이 제공되는 경우
    try:
        sql = f"SELECT snapshot_id FROM {catalog}.system.snapshots WHERE namespace = '{namespace}' AND table_name = '{table_name}' ORDER BY committed_at DESC LIMIT 1"
        rows = spark.sql(sql).collect()
        if rows:
            return str(rows[0][0])
    except Exception:
        pass

    # 시도 2: 테이블 메타데이터 접근 (spark._jvm 이용)
    try:
        jtable = spark._jsparkSession.table(f"{catalog}.{namespace}.{table_name}")
        # placeholder: environment-specific calls to access metadata
    except Exception:
        pass

    import uuid
    return f"snapshot-{uuid.uuid4()}"
"""