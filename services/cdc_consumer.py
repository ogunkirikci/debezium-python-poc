import os
import json
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
from psycopg2 import sql

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CDC_TOPIC = os.getenv("CDC_TOPIC", "debezium.public.customers")
LOG_FILE = os.getenv("LOG_FILE", "logs/cdc_updates.log")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")


def ensure_log_directory():
    log_dir = os.path.dirname(LOG_FILE)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)


def escape_sql_string(value):
    if isinstance(value, str):
        return value.replace("'", "''")
    return value


def format_sql_value(value):
    if value is None:
        return "NULL"
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, str):
        return f"'{escape_sql_string(value)}'"
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        return f"'{escape_sql_string(str(value))}'"


def find_primary_key(before, after):
    """Try to identify primary key from common naming patterns."""
    common_pk_names = ['id', 'pk', 'uuid', 'guid']
    
    for pk_name in common_pk_names:
        if pk_name in before and before[pk_name] is not None:
            return pk_name, before[pk_name]
        if pk_name in after and after[pk_name] is not None:
            return pk_name, after[pk_name]
    
    for key in before.keys():
        if key.endswith('_id') or key.endswith('_uuid') or key.endswith('_pk'):
            if before[key] is not None:
                return key, before[key]
    
    return None, None


def generate_where_clause(before, after):
    """Generate WHERE clause prioritizing primary keys."""
    where_clauses = []
    
    pk_name, pk_value = find_primary_key(before, after)
    
    if pk_name and pk_value is not None:
        where_clauses.append(f"{pk_name} = {format_sql_value(pk_value)}")
        return where_clauses
    
    if before:
        for key, value in before.items():
            if value is None:
                where_clauses.append(f"{key} IS NULL")
            else:
                where_clauses.append(f"{key} = {format_sql_value(value)}")
    
    return where_clauses


def generate_sql_query(source, before, after, op):
    schema = source.get("schema", "public")
    table = source.get("table", "unknown")
    full_table = f"{schema}.{table}"
    
    if op == "u":
        if not after:
            return "UPDATE query (after values missing)"
        
        set_clauses = []
        
        if before:
            for key in after.keys():
                if key in before and before[key] != after[key]:
                    set_clauses.append(f"{key} = {format_sql_value(after[key])}")
        else:
            for key, value in after.items():
                set_clauses.append(f"{key} = {format_sql_value(value)}")
        
        if not set_clauses:
            return f"UPDATE {full_table} (no changes detected)"
        
        where_clauses = generate_where_clause(before, after)
        
        if where_clauses:
            return f"UPDATE {full_table} SET {', '.join(set_clauses)} WHERE {' AND '.join(where_clauses)}"
        else:
            return f"UPDATE {full_table} SET {', '.join(set_clauses)} (WHERE clause could not be generated - before values missing)"
    
    elif op == "c" and after:
        columns = []
        values = []
        for key, value in after.items():
            columns.append(key)
            values.append(format_sql_value(value))
        return f"INSERT INTO {full_table} ({', '.join(columns)}) VALUES ({', '.join(values)})"
    
    elif op == "d" and before:
        where_clauses = generate_where_clause(before, None)
        if where_clauses:
            return f"DELETE FROM {full_table} WHERE {' AND '.join(where_clauses)}"
        return f"DELETE FROM {full_table} (WHERE clause could not be generated)"
    
    return "N/A"


def get_pg_connection():
    """Open a PostgreSQL connection using environment defaults."""
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    conn.autocommit = True
    return conn


def insert_row(cursor, schema, table, row):
    """Insert the 'after' payload into the target table."""
    columns = list(row.keys())
    query = sql.SQL("INSERT INTO {schema}.{table} ({cols}) VALUES ({vals})").format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
        cols=sql.SQL(", ").join(sql.Identifier(col) for col in columns),
        vals=sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    cursor.execute(query, [row[col] for col in columns])


def process_event(cursor, event):
    """Route CDC events to PostgreSQL inserts for create/read operations."""
    op = event.get("op")
    after = event.get("after")
    source = event.get("source", {})
    schema = source.get("schema", "public")
    table = source.get("table", "unknown")

    if op in ("c", "r") and after:
        insert_row(cursor, schema, table, after)


def log_update_event(event, message):
    if event.get("op") != "u":
        return
    
    source = event.get("source", {})
    before = event.get("before", {})
    after = event.get("after", {})
    
    db_name = source.get("db", "unknown")
    table_name = source.get("table", "unknown")
    timestamp = datetime.fromtimestamp(source.get("ts_ms", 0) / 1000).strftime("%Y-%m-%d %H:%M:%S")
    
    updated_by = after.get("updated_by") or after.get("modified_by") or before.get("updated_by") or "unknown"
    
    changed_fields = []
    for key in after.keys():
        if key in before and before[key] != after[key]:
            changed_fields.append({
                "field": key,
                "before": before[key],
                "after": after[key]
            })
    
    sql_query = generate_sql_query(source, before, after, "u")
    
    log_line = f"""
{'=' * 100}
[UPDATE EVENT] - {timestamp}
Database: {db_name} | Table: {table_name}
Updated By: {updated_by}
Partition: {message.partition} | Offset: {message.offset}
SQL Query:
{sql_query}
Changes:
{json.dumps(changed_fields, indent=2, ensure_ascii=False)}
Before Values:
{json.dumps(before, indent=2, ensure_ascii=False)}
After Values:
{json.dumps(after, indent=2, ensure_ascii=False)}
{'=' * 100}
"""
    
    print(log_line)
    
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_line)
        f.flush()


def main():
    ensure_log_directory()
    print(f"CDC consumer başlatılıyor... Topic: {CDC_TOPIC}")
    print(f"Log dosyası: {LOG_FILE}")
    print(f"PostgreSQL bağlantısı açılıyor... {PG_HOST}:{PG_PORT}/{PG_DB}")

    conn = get_pg_connection()
    cursor = conn.cursor()
    
    consumer = KafkaConsumer(
        CDC_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    try:
        for message in consumer:
            event = message.value
            process_event(cursor, event)
            log_update_event(event, message)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
