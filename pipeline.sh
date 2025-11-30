#!/bin/bash
#
# pipeline.sh : PostgreSQL → Kafka → Iceberg 파이프라인을 실행하고, 전송이 완료되면 자동으로 Connector를 언로드하는 스크립트.

CONNECTOR_POSTGRES="postgres-oorder"
CONNECTOR_ICEBERG="iceberg-oorder"
KAFKA_CONNECT_URL="http://localhost:8083"
SLEEP_TIME=30  # Source Connector 삭제 대기 시간 (최대 60초 + 안전 마진)
SLEEP_TIME_SINK=5  # Sink Connector 삭제 대기 시간 (Source 삭제 후 추가 5초 대기)

# 0. 환경 변수 설정 (AWS, Kafka, Confluent 등)
# export AWS_ACCESS_KEY_ID="..."
# export AWS_SECRET_ACCESS_KEY="..."
# export BOOTSTRAP_SERVERS="localhost:9092"
# export KAFKA_TOPIC="my_topic"

## 1. Confluent/Kafka Connect 서비스 시작
echo "🔸 Checking Confluent or Kafka Connect status..."
confluent local services start

# 2. PostgreSQL → Kafka Source Connector 실행
echo "🔸 Starting PostgreSQL Source Connector..."
confluent local services connect connector load $CONNECTOR_POSTGRES -c /Users/vldb/Downloads/p2i/postgres-source.json

# 3. Kafka → Iceberg Sink Connector 실행
echo "🔸 Starting Iceberg Sink Connector..."
confluent local services connect connector load $CONNECTOR_ICEBERG -c /Users/vldb/Downloads/p2i/iceberg-sink-rest.json

# 4. 연결 상태 확인
echo "🔸 Checking connector status..."
confluent local services connect connector status $CONNECTOR_POSTGRES
confluent local services connect connector status $CONNECTOR_ICEBERG

# 5. Source Connector가 데이터를 Kafka로 전송할 시간을 기다림
echo "⏳ Waiting for PostgreSQL → Kafka data transfer ($SLEEP_TIME seconds)..."
sleep $SLEEP_TIME

# 6. Source Connector 삭제
echo "🗑️ Unloading PostgreSQL Source Connector: $CONNECTOR_POSTGRES"
confluent local services connect connector unload $CONNECTOR_POSTGRES

# 7. Sink Connector가 Iceberg로 데이터를 커밋할 시간을 추가로 대기
echo "⏳ Waiting for Kafka → Iceberg data commit ($SLEEP_TIME_SINK seconds)..."
sleep $SLEEP_TIME_SINK

# 8. Sink Connector 삭제
echo "🗑️ Unloading Iceberg Sink Connector: $CONNECTOR_ICEBERG"
confluent local services connect connector unload $CONNECTOR_ICEBERG

echo "✅ Pipeline execution completed! All connectors are now unloaded."
exit 0
