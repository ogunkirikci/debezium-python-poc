import os
import json
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
QUERY_LOG_TOPIC = os.getenv("QUERY_LOG_TOPIC", "query-log")
CDC_TOPIC = os.getenv("CDC_TOPIC", "debezium.public.customers")
MATCHED_TOPIC = os.getenv("MATCHED_TOPIC", "matched-queries")
LOG_FILE = os.getenv("LOG_FILE", "logs/matched_queries.log")


def ensure_log_directory():
    log_dir = os.path.dirname(LOG_FILE)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)


class QueryMatcher:
    def __init__(self):
        self.query_cache = {}  # {txid: [queries]}
        self.cache_timeout = 300  # 5 dakika
        
    def add_query(self, query_log):
        """Add query log to cache."""
        txid = query_log.get('txid')
        if txid:
            if txid not in self.query_cache:
                self.query_cache[txid] = []
            self.query_cache[txid].append(query_log)
    
    def find_matching_query(self, cdc_event):
        """Find matching query for CDC event."""
        source = cdc_event.get('source', {})
        txid = source.get('txId')
        
        if not txid:
            return None
        
        if txid in self.query_cache:
            queries = self.query_cache[txid]
            table_name = source.get('table', '')
            
            # Aynı tablo için query'yi bul
            for query in queries:
                if query.get('table_name') == table_name:
                    return query
        
        return None
    
    def cleanup_old_cache(self):
        """Remove old entries from cache."""
        current_time = datetime.now().timestamp()
        keys_to_remove = []
        
        for txid, queries in self.query_cache.items():
            if queries and (current_time - queries[0].get('timestamp', 0)) > self.cache_timeout:
                keys_to_remove.append(txid)
        
        for key in keys_to_remove:
            del self.query_cache[key]


def log_matched_query(matched_data):
    """Log matched query and CDC event."""
    log_line = f"""
{'=' * 100}
[MATCHED QUERY] - {matched_data['timestamp']}
Database: {matched_data['database']} | Table: {matched_data['table']}
Operation: {matched_data['operation']}
User: {matched_data['user_name']} | App: {matched_data['application_name']}
Transaction ID: {matched_data['txid']}
SQL Query:
{matched_data['query_text'] or 'N/A'}
Changed Fields:
{json.dumps(matched_data['changed_fields'], indent=2, ensure_ascii=False)}
Before Values:
{json.dumps(matched_data['old_data'], indent=2, ensure_ascii=False)}
After Values:
{json.dumps(matched_data['new_data'], indent=2, ensure_ascii=False)}
{'=' * 100}
"""
    
    print(log_line)
    
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_line)
        f.flush()


def main():
    ensure_log_directory()
    print(f"Query matcher başlatılıyor...")
    print(f"Query log topic: {QUERY_LOG_TOPIC}")
    print(f"CDC topic: {CDC_TOPIC}")
    print(f"Matched topic: {MATCHED_TOPIC}")
    
    matcher = QueryMatcher()
    
    # Query log consumer
    query_consumer = KafkaConsumer(
        QUERY_LOG_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000
    )
    
    # CDC consumer
    cdc_consumer = KafkaConsumer(
        CDC_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000
    )
    
    # Matched query producer
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    try:
        while True:
            # Query log'ları oku ve cache'e ekle
            query_messages = query_consumer.poll(timeout_ms=100)
            for topic_partition, messages in query_messages.items():
                for message in messages:
                    query_log = message.value
                    matcher.add_query(query_log)
            
            # CDC event'lerini oku ve match'le
            cdc_messages = cdc_consumer.poll(timeout_ms=100)
            for topic_partition, messages in cdc_messages.items():
                for message in messages:
                    cdc_event = message.value
                    
                    if cdc_event.get("op") == "u":
                        matching_query = matcher.find_matching_query(cdc_event)
                        
                        if matching_query:
                            source = cdc_event.get("source", {})
                            before = cdc_event.get("before", {})
                            after = cdc_event.get("after", {})
                            
                            matched_data = {
                                'timestamp': datetime.fromtimestamp(
                                    source.get("ts_ms", 0) / 1000
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                'database': source.get("db", "unknown"),
                                'table': source.get("table", "unknown"),
                                'operation': matching_query.get('operation', 'UPDATE'),
                                'user_name': matching_query.get('user_name', 'unknown'),
                                'application_name': matching_query.get('application_name', 'unknown'),
                                'txid': matching_query.get('txid'),
                                'query_text': matching_query.get('query_text'),
                                'changed_fields': matching_query.get('changed_fields', {}),
                                'old_data': before,
                                'new_data': after
                            }
                            
                            log_matched_query(matched_data)
                            producer.send(MATCHED_TOPIC, value=matched_data)
                            producer.flush()
            
            # Cache temizleme
            matcher.cleanup_old_cache()
            
    except KeyboardInterrupt:
        print("\nDurduruluyor...")
    finally:
        query_consumer.close()
        cdc_consumer.close()
        producer.close()


if __name__ == "__main__":
    main()

