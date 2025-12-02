#!/bin/bash
# cloud-init snippet for EC2 instance to install Postgres, Python deps, Spark, DuckDB
set -eux
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y default-jre python3-pip postgresql postgresql-contrib
pip3 install --upgrade pip
pip3 install psycopg2-binary pyspark pyarrow s3fs duckdb pyiceberg
# Additional setup (spark/jars, environment variables, IAM role) should be configured per environment
