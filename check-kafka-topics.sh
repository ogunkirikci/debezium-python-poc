#!/bin/bash

# Kafka Topic'lerini ve mesajlarını kontrol etme scripti

KAFKA_BOOTSTRAP="localhost:9093"

echo "=== Kafka Topic'lerini Listeleme ==="
docker exec -it debezium-python-poc-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list

echo ""
echo "=== Debezium Topic'lerini Detaylı Görüntüleme ==="
docker exec -it debezium-python-poc-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list | grep debezium

echo ""
echo "=== Belirli bir topic'in detaylarını görüntüleme ==="
if [ "$1" != "" ]; then
  TOPIC_NAME=$1
  echo "Topic: $TOPIC_NAME"
  docker exec -it debezium-python-poc-kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic $TOPIC_NAME
  
  echo ""
  echo "=== Son 5 mesajı görüntüleme ==="
  docker exec -it debezium-python-poc-kafka-1 kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic $TOPIC_NAME \
    --from-beginning \
    --max-messages 5 \
    --timeout-ms 5000
else
  echo "Kullanım: $0 <topic_name>"
  echo "Örnek: $0 debezium.public.users"
fi

