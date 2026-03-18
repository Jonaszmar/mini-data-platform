#!/bin/bash
set -e

sleep 20

curl -i -X POST http://localhost:8083/connectors/ \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  -d '{
    "name": "postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "plugin.name": "pgoutput",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "business_db",
      "database.server.name": "dbserver1",
      "topic.prefix": "dbserver1",
      "table.include.list": "public.customers,public.products,public.orders",
      "slot.name": "debezium_slot",
      "publication.name": "dbz_publication",
      "publication.autocreate.mode": "filtered",
      "tombstones.on.delete": "false",
      "include.schema.changes": "false",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "true",
      "value.converter.schemas.enable": "true"
    }
  }'