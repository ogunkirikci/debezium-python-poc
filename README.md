# Debezium CDC Python Consumer

PostgreSQL'deki CDC (Debezium) event'lerini Kafka'dan okuyup:
- `op` tipi `c` veya `r` olan kayıtları hedef PostgreSQL tablosuna INSERT eder
- `op` tipi `u` olan kayıtların değişimlerini loglar

## Nasıl Çalışıyor?

Debezium, PostgreSQL WAL'dan değişiklikleri alır, Kafka'ya yazar. Python consumer bu topic'i dinler:
- `op = c` veya `op = r`: `payload.after` içeriğini, event'in `source.schema` ve `source.table` bilgisiyle hedef tabloya INSERT eder.
- `op = u`: Değişen alanları ve üretilen SQL örneğini loglar.

```
PostgreSQL → Debezium → Kafka → Python Consumer → PostgreSQL (+ logs/cdc_updates.log)
```

## Kurulum

### 1. Bağımlılıkları Kur

İsteğe bağlı sanal ortam açın, ardından:
```bash
pip install -r requirements.txt
```

### 2. Servisleri Başlat

```bash
docker-compose up -d
```

Zookeeper, Kafka, PostgreSQL ve Debezium Connect servisleri başlar.

### 3. Debezium Connector Kurulumu

Docker'daki PostgreSQL için hazır script var:

```bash
./setup-connector.sh
```

Local PostgreSQL kullanıyorsanız:

```bash
./setup-connector-local.sh <database> <table> <username> <password>
```

Örnek:
```bash
./setup-connector-local.sh mydb users postgres mypassword
```

**Not:** Local PostgreSQL için önce logical replication'ı aktif etmeniz gerekiyor:

1. `postgresql.conf` dosyasında:
```conf
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

2. PostgreSQL'de replication slot ve publication oluşturun:
```sql
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');
CREATE PUBLICATION debezium_publication FOR TABLE users;
```

### 4. Consumer'ı Çalıştır

Environment değişkenlerini (gerekirse) ayarlayın:
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export CDC_TOPIC=debezium.public.customers
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=postgres
export PG_USER=postgres
export PG_PASSWORD=postgres
```

Consumer'ı başlatın:
```bash
python services/cdc_consumer.py
```

### 5. Test

PostgreSQL'de bir UPDATE yapın:

```sql
UPDATE customers SET name = 'Yeni İsim', updated_by = 'admin' WHERE id = 1;
```

Log dosyasını izleyin:

```bash
tail -f logs/cdc_updates.log
```

## Connector Yönetimi

```bash
# Connector listesi
curl http://localhost:8083/connectors

# Connector durumu
curl http://localhost:8083/connectors/postgres-connector/status

# Connector silme
curl -X DELETE http://localhost:8083/connectors/postgres-connector
```

## Kafka Topic Kontrolü

```bash
# Topic listesi ve mesajları görüntüleme
./check-kafka-topics.sh debezium.public.users
```

## Yapılandırma

Ortam değişkenleri ile ayarlanabilir:

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka adresi (varsayılan: `localhost:9092`)
- `CDC_TOPIC`: İzlenecek topic (varsayılan: `debezium.public.customers`)
- `LOG_FILE`: Log dosyası yolu (varsayılan: `logs/cdc_updates.log`)
- `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD`: PostgreSQL bağlantı bilgileri (varsayılan: `localhost`, `5432`, `postgres`, `postgres`, `postgres`)

## Log Formatı

Her UPDATE event'inde şunlar loglanır:
- Timestamp
- Database ve tablo adı
- Kim tarafından güncellendi (`updated_by` veya `modified_by` alanından)
- Hangi alanlar değişti (önceki ve sonraki değerlerle)
- Tüm before/after değerleri

