import pandas as pd
from my_spider import MySpider
from db_operations import save_to_postgres
from scrapy.crawler import CrawlerProcess
import random
import dns.resolver
from urllib.parse import urlparse
import logging
import requests
import asyncio
from playwright.sync_api import sync_playwright
from utilities import random_user_agent, ignored_extensions
import psycopg2

# User-defined variables
websites = [
    'https://openai.com',
]
csv_file_name = 'scraped_data.csv'
database_name = "db_scrape"
database_user = "lesid_01"
database_password = "@Ketamine1324!"
database_host = "localhost"
database_port = "5432"

ignore_patterns = [
    "/blog?jk/authors",
    # Add more patterns here as needed
]
table_name = 'scraped_data'

min_delay = 0
max_delay = 0
crawl_level = 5
max_urls_to_scrape = 10

def get_visited_urls_from_database(database_config):
    dbname, user, password, host, port = database_config
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )

    cursor = conn.cursor()

    # Check if the visited_urls table exists, and create it if it doesn't
    cursor.execute("SELECT to_regclass('public.visited_urls');")
    table_exists = cursor.fetchone()[0]

    print(f"Table exists: {table_exists}")  # Add this print statement

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

    cursor.execute("SELECT url, scraped, attempts FROM visited_urls;")
    visited_urls = [(row[0], row[1], row[2]) for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return visited_urls

def main():
    pass

main()

logging.getLogger('charset_normalizer').setLevel(logging.WARNING)

visited_urls = get_visited_urls_from_database(
    (database_name, database_user, database_password, database_host, database_port))

# CrawlerProcess setup
process = CrawlerProcess({
    'USER_AGENT': random_user_agent(),
    'DOWNLOAD_DELAY': random.uniform(min_delay, max_delay),
    'DEPTH_LIMIT': crawl_level,
    'CLOSESPIDER_PAGECOUNT': max_urls_to_scrape if max_urls_to_scrape > 0 else None,
    'CLOSESPIDER_ITEMCOUNT': max_urls_to_scrape if max_urls_to_scrape > 0 else None,
})

crawler = process.create_crawler(MySpider)
process.crawl(crawler, websites=websites, csv_file_name=csv_file_name,
              database_config=(database_name, database_user, database_password, database_host, database_port),
              ignore_patterns=ignore_patterns, visited_urls=visited_urls)
process.start()

# Get the visited URLs from the Scrapy spider
visited_urls = crawler.spider.get_visited_urls()

# Run the dynamic scraper after the Scrapy spider is done
import dynamic_scraper


async def main_async():
    dynamic_df = await dynamic_scraper.scrape_dynamic_content(visited_urls, visited_urls)
    dynamic_scraper.update_database_with_dynamic_content(dynamic_df, table_name, (database_name, database_user, database_password, database_host, database_port))

asyncio.run(main_async())