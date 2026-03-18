# Mini Data Platform in Docker Containers

## Overview

This project simulates a business process and builds a mini data platform with:

- Python producer application
- PostgreSQL
- Debezium CDC
- Kafka
- Schema Registry
- Spark Structured Streaming
- MinIO Data Lake
- Delta Lake output

## Architecture
1. Python app generates random business data.
2. Data is inserted/updated in PostgreSQL.
3. Debezium captures DB changes using logical replication.
4. Changes are sent to Kafka in JSON format.
5. Kafka consumer validates JSON messages.
6. Spark reads events from Kafka.
7. Spark writes processed data to MinIO in Delta format.

## Run the project

### 1. Start all services

```bash
docker compose up --build -d

 register connection

curl -X POST http://localhost:8083/connectors ^
 -H "Content-Type: application/json" ^
 -d "{\"name\":\"postgres-connector\",\"config\":{\"connector.class\":\"io.debezium.connector.postgresql.PostgresConnector\",\"plugin.name\":\"pgoutput\",\"database.hostname\":\"postgres\",\"database.port\":\"5432\",\"database.user\":\"postgres\",\"database.password\":\"postgres\",\"database.dbname\":\"business_db\",\"database.server.name\":\"dbserver1\",\"topic.prefix\":\"dbserver1\",\"table.include.list\":\"public.customers,public.products,public.orders\",\"slot.name\":\"debezium_slot\",\"publication.name\":\"dbz_publication\",\"publication.autocreate.mode\":\"filtered\",\"tombstones.on.delete\":\"false\",\"include.schema.changes\":\"false\",\"key.converter\":\"org.apache.kafka.connect.json.JsonConverter\",\"value.converter\":\"org.apache.kafka.connect.json.JsonConverter\",\"key.converter.schemas.enable\":\"true\",\"value.converter.schemas.enable\":\"true\"}}"






### Check-ins

## connector
http://localhost:8083/connectors

## data Lake
http://localhost:9001