#!/bin/bash

# 다운로드 경로 설정
DOWNLOAD_DIR="/Users/vldb/Downloads/석사학위논문"

# JAR 파일 및 URL 매핑
declare -A JAR_FILES=(
  ["hadoop-aws-3.3.6.jar"]="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar"
  ["iceberg-aws-sdk-v2-2.20.56.jar"]="https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.56/iceberg-aws-sdk-v2-2.20.56.jar"
  ["iceberg-spark-runtime-3.5_2.12-1.7.1.jar"]="https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar"
  ["s3-tables-catalog-for-iceberg-runtime-0.1.3.jar"]="https://repo1.maven.org/maven2/software/amazon/s3tables/s3-tables-catalog-for-iceberg-runtime/0.1.3/s3-tables-catalog-for-iceberg-runtime-0.1.3.jar"
  ["commons-configuration2-2.11.0.jar"]="https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.11.0/commons-configuration2-2.11.0.jar"
  ["caffeine-3.1.8.jar"]="https://repo1.maven.org/maven2/com/github/ben-manes/caffeine/caffeine/3.1.8/caffeine-3.1.8.jar"
)

# 다운로드 실행
echo "다운로드 디렉토리: $DOWNLOAD_DIR"

for JAR_NAME in "${!JAR_FILES[@]}"; do
  JAR_URL="${JAR_FILES[$JAR_NAME]}"
  echo "Downloading $JAR_NAME from $JAR_URL..."
  curl -o "$DOWNLOAD_DIR/$JAR_NAME" "$JAR_URL"
  if [[ $? -eq 0 ]]; then
    echo "Successfully downloaded: $JAR_NAME"
  else
    echo "Failed to download: $JAR_NAME"
  fi
done

echo "모든 JAR 파일 다운로드 완료!"
