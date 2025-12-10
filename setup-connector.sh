#!/bin/bash

CONNECTOR_NAME="postgres-connector"
CONNECT_URL="http://localhost:8083/connectors"

echo "Connector kurulumu başlatılıyor..."

CONNECTOR_CONFIG='{
CONNECTOR_CONFIG='{
  "name": "'$CONNECTOR_NAME'",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "debezium",
    "database.dbname": "inventory",
    "database.server.name": "debezium",
    "table.include.list": "public.customers",
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

