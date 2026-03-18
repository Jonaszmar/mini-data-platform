#!/bin/bash
set -e

until kafka-topics --bootstrap-server kafka:29092 --list > /dev/null 2>&1; do
  echo "Waiting for Kafka..."
  sleep 3
done

echo "Kafka is ready."