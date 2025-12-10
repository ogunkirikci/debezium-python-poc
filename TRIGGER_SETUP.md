# PostgreSQL Trigger ile Query Yakalama

Bu yaklaşım, PostgreSQL trigger'ları kullanarak query'leri yakalar ve Debezium event'leri ile match'ler.

## Nasıl Çalışır?

```
Application → PostgreSQL → Trigger → query_log tablosu
                                      ↓
                              trigger_query_reader.py
                                      ↓
                                    Kafka
                                      ↓
                              query_matcher.py
                                      ↓
                            Debezium Event + Query
                                      ↓
                              Final Log
```

## Kurulum

### 1. PostgreSQL'de Trigger Kurulumu

```bash
psql -U postgres -d your_database -f sql/trigger_setup.sql
```

Bu script:
- `query_log` tablosunu oluşturur
- Trigger function'ı oluşturur
- İstediğiniz tablolara trigger ekler

### 2. Trigger'ı Belirli Tablolara Ekle

```sql
-- Örnek: users tablosu için
CREATE TRIGGER log_users_queries
    AFTER INSERT OR UPDATE OR DELETE ON users
    FOR EACH ROW
    EXECUTE FUNCTION log_query_trigger();
```

### 3. Query Log Reader'ı Başlat

```bash
cd services
python trigger_query_reader.py
```

Bu script:
- `query_log` tablosunu sürekli okur
- Yeni query'leri Kafka'ya gönderir

### 4. Query Matcher'ı Başlat

```bash
cd services
python query_matcher.py
```

Bu script:
- Query log'ları Kafka'dan okur
- Debezium event'lerini Kafka'dan okur
- Transaction ID ile match'ler
- Final log oluşturur

## Ortam Değişkenleri

### trigger_query_reader.py
- `DB_HOST`: PostgreSQL host (varsayılan: localhost)
- `DB_PORT`: PostgreSQL port (varsayılan: 5432)
- `DB_NAME`: Database adı
- `DB_USER`: Database kullanıcısı
- `DB_PASSWORD`: Database şifresi
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka adresi (varsayılan: localhost:9092)
- `QUERY_LOG_TOPIC`: Query log topic'i (varsayılan: query-log)
- `POLL_INTERVAL`: Polling interval saniye (varsayılan: 1)

### query_matcher.py
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka adresi (varsayılan: localhost:9092)
- `QUERY_LOG_TOPIC`: Query log topic'i (varsayılan: query-log)
- `CDC_TOPIC`: Debezium CDC topic'i (varsayılan: debezium.public.customers)
- `MATCHED_TOPIC`: Matched query topic'i (varsayılan: matched-queries)
- `LOG_FILE`: Log dosyası yolu (varsayılan: logs/matched_queries.log)

## Sınırlamalar

1. **Query Text:**
   - `current_query()` trigger context'inde sınırlı çalışır
   - Karmaşık query'lerde tam query'yi yakalayamayabilir
   - Alternatif: `log_statement = 'all'` kullan ve log dosyasını parse et

2. **Performance:**
   - Her row için trigger çalışır
   - Yüksek trafikli sistemlerde performans etkisi olabilir
   - Batch insert/update'lerde her row için ayrı trigger çalışır

3. **Transaction Context:**
   - Trigger transaction içinde çalışır
   - Rollback olursa query_log'a yazılan kayıt da kaybolur
   - Debezium event'i gelmeyebilir (rollback durumunda)

## Alternatif: log_statement Kullanımı

Trigger yerine PostgreSQL'in built-in query logging'ini kullanabilirsin:

```sql
-- postgresql.conf
log_statement = 'all'
log_destination = 'csvlog'
logging_collector = on
```

Sonra log dosyasını parse edip Kafka'ya gönderebilirsin.

## Test

1. PostgreSQL'de bir UPDATE yap:
```sql
UPDATE users SET name = 'Yeni İsim' WHERE id = 1;
```

2. Query log'u kontrol et:
```sql
SELECT * FROM query_log ORDER BY id DESC LIMIT 1;
```

3. Matched log'u kontrol et:
```bash
tail -f logs/matched_queries.log
```

