제목: migration: idempotent snapshot reads, migration_history schema, benchmark harness

요약:
- Postgres → Iceberg 마이그레이션의 일관성과 안전성을 보강했습니다.
- 주요 변경:
  - migration_history, migration_outbox 스키마 추가 (db/migration_schema.sql)
  - REPEATABLE READ 기반의 snapshot-consistent 읽기, batch idempotency, migration_history 업데이트 로직 (migration.py)
  - Postgres에서 타입 안전한 임시 테이블을 생성해 PK 기반 삭제를 수행하도록 개선(migration_improved.py 제안)
  - 간단 벤치마크 러너 추가 (bench/run_benchmark.py)

검증 방법:
1. db/migration_schema.sql 적용:
   psql "<connection-string>" -f db/migration_schema.sql
2. 대상 테이블(orders, order_line, hhistory 등)에 트리거 적용(원하면 스크립트 제공)
   예: CREATE TRIGGER trg_prevent_updates_orders BEFORE UPDATE OR DELETE ON orders FOR EACH ROW EXECUTE FUNCTION prevent_updates_on_sealed_rows();
3. migration.py 실행(환경변수/DSN/Spark 설정 확인)
   python3 migration.py
   - migration_history 테이블 상태 확인
   - S3 Iceberg에 데이터가 append 되었는지 확인
4. bench/run_benchmark.py로 간단 비교 실행

유의 사항:
- Iceberg snapshot id 추출 로직은 카탈로그(REST/Hive/Glue)와 Iceberg/Spark 버전에 따라 조정이 필요합니다.
- 대용량 데이터에서 Parquet 생성/업로드/append는 스트리밍/분할 처리가 필요합니다.
- migration 워커는 장애 복구 및 재시도 로직(백오프, 알림)을 추가로 보강할 것을 권장합니다.
