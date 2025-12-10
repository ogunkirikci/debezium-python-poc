import os
import json
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer
from datetime import datetime

DB_CONFIG = {
    'host': os.getenv("DB_HOST", "localhost"),
    'port': os.getenv("DB_PORT", "5432"),
    'database': os.getenv("DB_NAME", "your_database"),
    'user': os.getenv("DB_USER", "postgres"),
    'password': os.getenv("DB_PASSWORD", "postgres")
}

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
QUERY_LOG_TOPIC = os.getenv("QUERY_LOG_TOPIC", "query-log")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "1"))


def get_query_logs(conn, last_id=0):
    """Get new query logs from database."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT 
                id,
                table_name,
                operation,
                query_text,
                old_data,
                new_data,
                changed_fields,
                user_name,
                application_name,
                transaction_id,
                timestamp,
                txid
            FROM query_log
            WHERE id > %s
            ORDER BY id ASC
            LIMIT 100
        """, (last_id,))
        
        return cur.fetchall()


def send_to_kafka(producer, query_log):
    """Send query log to Kafka."""
    message = {
        'id': query_log['id'],
        'table_name': query_log['table_name'],
        'operation': query_log['operation'],
        'query_text': query_log['query_text'],
        'old_data': query_log['old_data'],
        'new_data': query_log['new_data'],
        'changed_fields': query_log['changed_fields'],
        'user_name': query_log['user_name'],
        'application_name': query_log['application_name'],
        'transaction_id': query_log['transaction_id'],
        'timestamp': query_log['timestamp'].isoformat() if query_log['timestamp'] else None,
        'txid': query_log['txid']
    }
    
    producer.send(QUERY_LOG_TOPIC, value=json.dumps(message).encode('utf-8'))
    producer.flush()


def main():
    print(f"Query log reader başlatılıyor...")
    print(f"Database: {DB_CONFIG['database']}")
    print(f"Kafka topic: {QUERY_LOG_TOPIC}")
    
    # Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v
    )
    
    # PostgreSQL connection
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    
    last_id = 0
    
    try:
        while True:
            query_logs = get_query_logs(conn, last_id)
            
            for query_log in query_logs:
                send_to_kafka(producer, query_log)
                last_id = max(last_id, query_log['id'])
                print(f"Query log gönderildi: ID={query_log['id']}, Table={query_log['table_name']}, Operation={query_log['operation']}")
            
            time.sleep(POLL_INTERVAL)
            
    except KeyboardInterrupt:
        print("\nDurduruluyor...")
    finally:
        conn.close()
        producer.close()


if __name__ == "__main__":
    main()

