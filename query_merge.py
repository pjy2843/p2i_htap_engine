from sqlglot import parse_one, exp

def rewrite_tables_with_union(query: str, table_names: list[str]) -> str:
    tree = parse_one(query)

    for table in tree.find_all(exp.Table):
        original_name = table.name
        if original_name in table_names:
            # Construct the UNION ALL subquery
            union_query = f"(SELECT * FROM {original_name}_pg UNION ALL SELECT * FROM {original_name}_ice)"
            subquery = parse_one(union_query)
            table.replace(exp.Subquery(this=subquery, alias=table.alias or exp.to_identifier(original_name)))

    return tree.sql()

# 예시 쿼리 테스트
example_query = """
SELECT sum(ol.ol_amount)
FROM order_line ol
JOIN oorder o ON ol.ol_o_id = o.o_id
WHERE ol.ol_delivery_d >= CURRENT_DATE - INTERVAL '7 days'
"""

# 적용 대상 테이블 이름
tables = ["order_line", "oorder"]

# 리라이트된 쿼리 결과
rewritten_sql = rewrite_tables_with_union(example_query, tables)
print(rewritten_sql)
