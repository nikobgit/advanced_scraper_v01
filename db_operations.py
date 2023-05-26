from psycopg2 import sql
import logging
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import re
import psycopg2
import pandas as pd
import json


logging.basicConfig(level=logging.WARNING)


def extract_root_domain(url):
    from urllib.parse import urlparse
    parsed_url = urlparse(url)
    if parsed_url.hostname is not None:
        url_domain_parts = parsed_url.hostname.split('.')
        url_root_domain = '.'.join(url_domain_parts[-2:])
        # Replace periods with underscores and convert to lowercase for PostgreSQL table names
        sanitized_root_domain = url_root_domain.replace('.', '_').lower()
        return sanitized_root_domain
    else:
        return None

def sanitize_string(string):
    return re.sub(r'\W|^(?=\d)', '_', string)


def get_scraped_urls_from_database(table_name, dbname, user, password, host, port):
    table_name = sanitize_string(table_name)
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    cursor = conn.cursor()
    cursor.execute(f"SELECT to_regclass('public.{table_name}');")
    table_exists = cursor.fetchone()[0]
    if table_exists:
        query = f"SELECT url FROM {table_name};"
        cursor.execute(query)
        urls = [row[0] for row in cursor.fetchall()]
    else:
        # Create the table if it doesn't exist
        create_table_if_not_exists(conn, table_name, pd.DataFrame(columns=['url', 'content', 'tables', 'code_snippets', 'domain']))
        urls = []
    cursor.close()
    conn.close()
    return urls


def create_database_if_not_exists(database, user, password, host, port):
    conn = psycopg2.connect(
        dbname="postgres",
        user=user,
        password=password,
        host=host,
        port=port,
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname='{database}';")
    exists = cursor.fetchone()
    if not exists:
        cursor.execute(f"CREATE DATABASE {database};")
    cursor.close()
    conn.close()


def create_table_if_not_exists(conn, table_name, df):
    table_name = sanitize_string(table_name)
    cursor = conn.cursor()
    cursor.execute(f"SELECT to_regclass('public.{table_name}');")
    table_exists = cursor.fetchone()[0]
    if not table_exists:
        schema = df.dtypes.map(lambda x: x.name).to_dict()
        schema_sql = ", ".join([f"{col} {dtype.upper()}" for col, dtype in schema.items()])
        schema_sql = schema_sql.replace("OBJECT", "TEXT")
        create_table_sql = f"""
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                domain TEXT,
                url TEXT UNIQUE,
                content TEXT,
                dynamic_content TEXT,
                tables TEXT,
                code_snippets TEXT
            );
        """
        cursor.execute(create_table_sql)
        conn.commit()
    cursor.close()


def save_to_postgres(df, table_name, database, user, password, host, port, query=None, recreate_table=False):
    table_name = sanitize_string(table_name)
    create_database_if_not_exists(database, user, password, host, port)
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    if query is None:
        query = sql.SQL("""
                INSERT INTO {0} (domain, url, content, tables, code_snippets)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (url) DO UPDATE SET
                content = EXCLUDED.content,
                tables = EXCLUDED.tables,
                code_snippets = EXCLUDED.code_snippets;
            """).format(sql.Identifier(table_name))

    if recreate_table:
        drop_table_if_exists(conn, table_name)

    create_table_if_not_exists(conn, table_name, df)

    cursor = conn.cursor()

    for _, row in df.iterrows():
        query = sql.SQL("""
               INSERT INTO {0} (domain, url, content, dynamic_content, tables, code_snippets)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (url) DO UPDATE SET
               content = EXCLUDED.content,
               tables = EXCLUDED.tables,
               code_snippets = EXCLUDED.code_snippets;
           """).format(sql.Identifier(table_name))

        cursor.execute(query, (
            row['domain'], row['url'], row['content'], "", row['tables'], row['code_snippets']))
        conn.commit()

    cursor.close()
    conn.close()


def drop_table_if_exists(conn, table_name):
    table_name = extract_root_domain(table_name)
    cursor = conn.cursor()

    cursor.execute(f"SELECT to_regclass('public.{table_name}');")
    table_exists = cursor.fetchone()[0]

    if table_exists:
        drop_table_sql = f"DROP TABLE {table_name};"
        cursor.execute(drop_table_sql)
        conn.commit()

    cursor.close()


def update_database_with_dynamic_content(df, table_name, database_config):
    dbname, user, password, host, port = database_config
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )

    cursor = conn.cursor()

    for _, row in df.iterrows():
        # Concatenate dynamic content from scroll and click/expand/modals
        combined_dynamic_content = row['dynamic_content_scroll'] + " " + row['dynamic_content_click']
        combined_tables = row['tables_scroll'] + row['tables_click']
        combined_code_snippets = row['code_snippets_scroll'] + row['code_snippets_click']

        query = f"""
            UPDATE {table_name}
            SET dynamic_content = %s,
                tables = %s::jsonb,
                code_snippets = %s::jsonb
            WHERE url = %s;
        """
        cursor.execute(query, (
            combined_dynamic_content, json.dumps(combined_tables), json.dumps(combined_code_snippets), row['url']))
        conn.commit()

    cursor.close()
    conn.close()


def update_database_with_visited_urls(visited_urls, table_name, database_config):
    dbname, user, password, host, port = database_config
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )

    cursor = conn.cursor()

    for url in visited_urls:
        query = f"""
            INSERT INTO {table_name} (url)
            VALUES (%s)
            ON CONFLICT (url) DO NOTHING;
        """
        cursor.execute(query, (url,))
        conn.commit()

    cursor.close()
    conn.close()