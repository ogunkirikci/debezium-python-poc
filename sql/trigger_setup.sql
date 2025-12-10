-- PostgreSQL Trigger ile Query Yakalama
-- Bu trigger'lar query'leri bir tabloya kaydeder ve Kafka'ya gönderilebilir

-- 1. Query log tablosu oluştur
CREATE TABLE IF NOT EXISTS query_log (
    id BIGSERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL, -- INSERT, UPDATE, DELETE
    query_text TEXT,
    old_data JSONB,
    new_data JSONB,
    changed_fields JSONB,
    user_name TEXT,
    application_name TEXT,
    transaction_id BIGINT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    txid BIGINT -- Transaction ID for matching with Debezium
);

-- Index for matching with Debezium events
CREATE INDEX IF NOT EXISTS idx_query_log_txid ON query_log(txid, timestamp);

-- 2. Trigger function - Query'yi yakalama
CREATE OR REPLACE FUNCTION log_query_trigger()
RETURNS TRIGGER AS $$
DECLARE
    v_query TEXT;
    v_txid BIGINT;
    v_user TEXT;
    v_app TEXT;
    v_changed_fields JSONB := '{}'::JSONB;
    v_key TEXT;
BEGIN
    -- Transaction ID'yi al (Debezium ile match için)
    v_txid := txid_current();
    
    -- User ve application bilgilerini al
    v_user := current_user;
    v_app := current_setting('application_name', true);
    
    -- Query'yi yakalamaya çalış
    -- Not: current_query() trigger context'inde sınırlı çalışır
    -- pg_stat_statements veya log_statement kullanmak daha iyi olabilir
    BEGIN
        v_query := current_query();
    EXCEPTION
        WHEN OTHERS THEN
            v_query := NULL;
    END;
    
    -- UPDATE için değişen alanları bul
    IF TG_OP = 'UPDATE' THEN
        FOR v_key IN SELECT jsonb_object_keys(to_jsonb(NEW)) LOOP
            IF (to_jsonb(OLD)->>v_key) IS DISTINCT FROM (to_jsonb(NEW)->>v_key) THEN
                v_changed_fields := v_changed_fields || jsonb_build_object(
                    v_key, jsonb_build_object(
                        'old', to_jsonb(OLD)->v_key,
                        'new', to_jsonb(NEW)->v_key
                    )
                );
            END IF;
        END LOOP;
    END IF;
    
    -- Query log'a kaydet
    INSERT INTO query_log (
        table_name,
        operation,
        query_text,
        old_data,
        new_data,
        changed_fields,
        user_name,
        application_name,
        txid
    ) VALUES (
        TG_TABLE_NAME,
        TG_OP,
        v_query,
        CASE WHEN TG_OP IN ('UPDATE', 'DELETE') THEN to_jsonb(OLD) ELSE NULL END,
        CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN to_jsonb(NEW) ELSE NULL END,
        v_changed_fields,
        v_user,
        v_app,
        v_txid
    );
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 3. Trigger'ı tablolara ekle
-- Örnek: users tablosu için
-- CREATE TRIGGER log_users_queries
--     AFTER INSERT OR UPDATE OR DELETE ON users
--     FOR EACH ROW
--     EXECUTE FUNCTION log_query_trigger();

-- 4. Tüm public schema'daki tablolara trigger eklemek için
-- (Dikkatli kullan, production'da performans etkisi olabilir)
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public'
    LOOP
        EXECUTE format('
            DROP TRIGGER IF EXISTS log_%s_queries ON %I;
            CREATE TRIGGER log_%s_queries
                AFTER INSERT OR UPDATE OR DELETE ON %I
                FOR EACH ROW
                EXECUTE FUNCTION log_query_trigger();
        ', r.tablename, r.tablename, r.tablename, r.tablename);
    END LOOP;
END $$;

-- 5. Query log'u Kafka'ya göndermek için bir function
-- (PostgreSQL'den direkt Kafka'ya göndermek için pg_kafka extension gerekir)
-- Alternatif: Python script ile query_log tablosunu okuyup Kafka'ya gönder

-- 6. Query log'u temizleme (opsiyonel)
-- DELETE FROM query_log WHERE timestamp < NOW() - INTERVAL '7 days';

