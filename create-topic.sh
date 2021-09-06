#!/usr/bin/env bash

docker-compose exec broker \
  kafka-topics --create \
    --topic $1 \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1