# Trigger vs Debezium Logları - Karşılaştırma

## Temel Farklar

### 1. **Kaynak ve Çalışma Zamanı**

**Trigger:**
- Database seviyesinde çalışır
- Query çalıştırıldığında **hemen** tetiklenir
- Transaction içinde çalışır
- Application'ın query göndermesiyle tetiklenir

**Debezium:**
- WAL (Write-Ahead Log) seviyesinde çalışır
- Değişiklik WAL'a yazıldıktan **sonra** yakalanır
- Transaction commit edildikten sonra işlenir
- Database'in internal mekanizmasıyla çalışır

### 2. **Yakalanan Bilgiler**

#### Trigger'dan Gelen Bilgiler:

```json
{
  "id": 123,
  "table_name": "users",
  "operation": "UPDATE",
  "query_text": "UPDATE users SET name = 'Yeni' WHERE id = 1",
  "old_data": {
    "id": 1,
    "name": "Eski",
    "email": "old@example.com"
  },
  "new_data": {
    "id": 1,
    "name": "Yeni",
    "email": "old@example.com"
  },
  "changed_fields": {
    "name": {
      "old": "Eski",
      "new": "Yeni"
    }
  },
  "user_name": "postgres",
  "application_name": "psql",
  "transaction_id": 12345,
  "timestamp": "2024-01-15 10:30:45",
  "txid": 12345
}
```

**Avantajlar:**
- ✅ **SQL Query'yi içerir** (tam query metni)
- ✅ **Application bilgisi** (application_name)
- ✅ **User bilgisi** (current_user)
- ✅ **Değişen alanlar** (hangi field'lar değişti)
- ✅ **Transaction ID** (match için)

**Dezavantajlar:**
- ❌ Query text her zaman tam değil (`current_query()` sınırlı)
- ❌ Her row için ayrı trigger çalışır (performance)
- ❌ Transaction rollback olursa kayıt kaybolur

#### Debezium'dan Gelen Bilgiler:

```json
{
  "op": "u",
  "before": {
    "id": 1,
    "name": "Eski",
    "email": "old@example.com"
  },
  "after": {
    "id": 1,
    "name": "Yeni",
    "email": "old@example.com"
  },
  "source": {
    "version": "2.6.2.Final",
    "connector": "postgresql",
    "name": "debezium",
    "ts_ms": 1705312245000,
    "snapshot": "false",
    "db": "mydb",
    "sequence": ["26907336", "26907392"],
    "schema": "public",
    "table": "users",
    "txId": 12345,
    "lsn": 26907392,
    "xmin": null
  },
  "ts_ms": 1705312245000
}
```

**Avantajlar:**
- ✅ **WAL seviyesinde** (çok güvenilir)
- ✅ **LSN (Log Sequence Number)** - tam sıralama
- ✅ **Transaction ID** (txId)
- ✅ **Timestamp** (milisaniye hassasiyetinde)
- ✅ **Database metadata** (db, schema, table)
- ✅ **Rollback durumunda** event gelmez (güvenilir)

**Dezavantajlar:**
- ❌ **SQL Query yok** (sadece before/after değerleri)
- ❌ **Application bilgisi yok**
- ❌ **User bilgisi yok** (bazı durumlarda)
- ❌ **Değişen alanlar** otomatik hesaplanmaz

### 3. **Çalışma Senaryoları**

#### Senaryo 1: Basit UPDATE

```sql
UPDATE users SET name = 'Yeni' WHERE id = 1;
```

**Trigger:**
- Query text: ✅ "UPDATE users SET name = 'Yeni' WHERE id = 1"
- Old/New: ✅ Var
- Application: ✅ Var
- User: ✅ Var

**Debezium:**
- Query text: ❌ Yok
- Old/New: ✅ Var
- Application: ❌ Yok
- User: ❌ Yok

#### Senaryo 2: Batch UPDATE

```sql
UPDATE users SET status = 'active' WHERE created_at < '2024-01-01';
-- 1000 satır etkilenir
```

**Trigger:**
- 1000 kez çalışır (her row için)
- Her biri için ayrı kayıt
- Performance etkisi yüksek

**Debezium:**
- 1000 event gönderir
- Her biri için ayrı event
- Daha verimli (WAL seviyesinde)

#### Senaryo 3: Transaction Rollback

```sql
BEGIN;
UPDATE users SET name = 'Yeni' WHERE id = 1;
ROLLBACK;
```

**Trigger:**
- Trigger çalışır ve query_log'a yazar
- Ama transaction rollback olursa kayıt kaybolur (autocommit yoksa)
- Veya kayıt kalır ama değişiklik olmamış olur

**Debezium:**
- Event gelmez (WAL'a yazılmadığı için)
- Daha güvenilir (sadece commit edilen değişiklikler)

#### Senaryo 4: Karmaşık Query

```sql
UPDATE users u 
SET name = (SELECT name FROM users_backup WHERE id = u.id)
WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = u.id);
```

**Trigger:**
- `current_query()` sadece trigger'ı tetikleyen kısmı gösterir
- Tam query'yi yakalayamayabilir

**Debezium:**
- Sadece etkilenen satırların before/after değerlerini gösterir
- Query'yi hiç göstermez

### 4. **Performance Etkisi**

**Trigger:**
- Her row için çalışır
- INSERT INTO query_log yapar (I/O)
- Transaction içinde çalışır (lock)
- Yüksek trafikli sistemlerde performans sorunu olabilir

**Debezium:**
- WAL'ı pasif olarak okur (düşük overhead)
- Database'in normal işleyişini etkilemez
- Daha verimli

### 5. **Güvenilirlik**

**Trigger:**
- Application query gönderdiğinde çalışır
- Rollback durumunda tutarsızlık olabilir
- Trigger disable edilirse çalışmaz

**Debezium:**
- WAL seviyesinde çalışır (çok güvenilir)
- Sadece commit edilen değişiklikleri yakalar
- Database'in core mekanizması

### 6. **Kullanım Senaryoları**

**Trigger Kullan:**
- ✅ SQL Query'yi görmek istiyorsan
- ✅ Application/User bilgisi gerekiyorsa
- ✅ Hangi query'nin hangi değişikliği yaptığını bilmek istiyorsan
- ✅ Audit log için (kim ne yaptı)

**Debezium Kullan:**
- ✅ Sadece değişiklikleri yakalamak istiyorsan
- ✅ High-performance gerekiyorsa
- ✅ Query'ye ihtiyaç yoksa
- ✅ CDC (Change Data Capture) için

**İkisini Birlikte Kullan:**
- ✅ Query + Debezium event = En kapsamlı log
- ✅ Transaction ID ile match'le
- ✅ Hem query hem de değişiklik bilgisi

## Özet Tablo

| Özellik | Trigger | Debezium |
|---------|---------|----------|
| **SQL Query** | ✅ (sınırlı) | ❌ |
| **Before/After** | ✅ | ✅ |
| **Application Info** | ✅ | ❌ |
| **User Info** | ✅ | ❌ |
| **Transaction ID** | ✅ | ✅ |
| **LSN** | ❌ | ✅ |
| **Performance** | ⚠️ (yavaş) | ✅ (hızlı) |
| **Güvenilirlik** | ⚠️ (rollback sorunu) | ✅ (çok güvenilir) |
| **WAL Seviyesi** | ❌ | ✅ |
| **Batch Operations** | ⚠️ (her row için) | ✅ (verimli) |

## Sonuç

**Trigger:**
- Query bilgisi için kullan
- Application/User context için kullan
- Audit log için kullan

**Debezium:**
- Değişiklik yakalama için kullan
- High-performance gerektiğinde kullan
- Güvenilir CDC için kullan

**İkisini Birlikte:**
- En kapsamlı çözüm
- Query + Değişiklik bilgisi
- Transaction ID ile match'le

