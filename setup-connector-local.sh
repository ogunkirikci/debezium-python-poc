#!/bin/bash

if [ "$#" -lt 4 ]; then
  echo "Kullanım: $0 <database> <table> <username> <password> [host] [port]"
  echo "Örnek: $0 mydb users postgres mypassword"
  exit 1
fi

DB_NAME=$1
TABLE_NAME=$2
DB_USER=$3
DB_PASSWORD=$4
DB_HOST=${5:-"host.docker.internal"}
DB_PORT=${6:-"5432"}

CONNECTOR_NAME="postgres-local-connector"
CONNECT_URL="http://localhost:8083/connectors"

echo "Local PostgreSQL connector kurulumu..."
echo "Database: $DB_NAME | Table: $TABLE_NAME | Host: $DB_HOST:$DB_PORT"

CONNECTOR_CONFIG='{
  "name": "'$CONNECTOR_NAME'",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "'$DB_HOST'",
    "database.port": "'$DB_PORT'",
    "database.user": "'$DB_USER'",
    "database.password": "'$DB_PASSWORD'",
    "database.dbname": "'$DB_NAME'",
    "database.server.name": "debezium",
    "table.include.list": "public.'$TABLE_NAME'",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "debezium_publication"
  }
}'

EXISTING=$(curl -s "$CONNECT_URL/$CONNECTOR_NAME" 2>/dev/null)

if [ "$EXISTING" != "" ] && [ "$EXISTING" != "Not Found" ]; then
  echo "Mevcut connector güncelleniyor..."
  curl -X PUT "$CONNECT_URL/$CONNECTOR_NAME/config" \
    -H "Content-Type: application/json" \
    -d "$(echo $CONNECTOR_CONFIG | jq '.config')"
else
  echo "Yeni connector oluşturuluyor..."
  curl -X POST "$CONNECT_URL" \
    -H "Content-Type: application/json" \
    -d "$CONNECTOR_CONFIG"
fi

echo ""
sleep 2
curl -s "$CONNECT_URL/$CONNECTOR_NAME/status" | jq '.'

echo ""
echo "Not: PostgreSQL'de logical replication aktif olmalı:"
echo "  - postgresql.conf: wal_level = logical"
echo "  - SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');"
echo "  - CREATE PUBLICATION debezium_publication FOR TABLE $TABLE_NAME;"

