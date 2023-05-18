import psycopg2
from psycopg2 import sql
import pandas as pd
import logging
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

logging.basicConfig(level=logging.WARNING)


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


def create_visited_urls_table_if_not_exists(conn):
    cursor = conn.cursor()

    cursor.execute("SELECT to_regclass('public.visited_urls');")
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        create_table_sql = """
            CREATE TABLE visited_urls (
                id SERIAL PRIMARY KEY,
                url TEXT UNIQUE,
                scraped BOOLEAN DEFAULT FALSE,
                attempts INTEGER DEFAULT 0
            );
        """
        cursor.execute(create_table_sql)
        conn.commit()

    cursor.close()



def save_visited_url_to_postgres(url, conn):
    cursor = conn.cursor()

    query = """
        INSERT INTO visited_urls (url)
        VALUES (%s)
        ON CONFLICT (url) DO UPDATE SET
          attempts = visited_urls.attempts + 1;
    """
    cursor.execute(query, (url,))
    conn.commit()

    cursor.close()


def save_to_postgres(df, table_name, database, user, password, host, port, query=None, recreate_table=False):
    create_database_if_not_exists(database, user, password, host, port)

    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port,
    )

    if recreate_table:
        drop_table_if_exists(conn, table_name)

    create_table_if_not_exists(conn, table_name, df)
    create_visited_urls_table_if_not_exists(conn)

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
        save_visited_url_to_postgres(row['url'], conn)

    cursor.close()
    conn.close()

def drop_table_if_exists(conn, table_name):
    cursor = conn.cursor()

    cursor.execute(f"SELECT to_regclass('public.{table_name}');")
    table_exists = cursor.fetchone()[0]

    if table_exists:
        drop_table_sql = f"DROP TABLE {table_name};"
        cursor.execute(drop_table_sql)
        conn.commit()

    cursor.close()

def create_table_if_not_exists(conn, table_name, df):
    cursor = conn.cursor()

    cursor.execute(f"SELECT to_regclass('public.{table_name}');")
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        schema = df.dtypes.map(lambda x: x.name).to_dict()
        schema_sql = ", ".join([f"{col} {dtype.upper()}" for col, dtype in schema.items()])
        schema_sql = schema_sql.replace("OBJECT", "TEXT")  # Change JSONB to TEXT

        # Update the table schema
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