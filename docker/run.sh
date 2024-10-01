#!/bin/bash

docker build -q ./db | \
 xargs docker run \
 -e CLICKHOUSE_USER=user \
 -e CLICKHOUSE_PASSWORD=password \
 --name test_clickhouse \
 --ulimit nofile=262144:262144 \
 -p 8123:8123 \
 -p 9000:9000