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
from db_operations import get_scraped_urls_from_database, extract_root_domain, sanitize_string
import db_operations


def main():
    pass


main()

logging.getLogger('charset_normalizer').setLevel(logging.WARNING)

# User-defined variables
websites = [
    'https://openai.com',
]
database_name = "db_scrape"
database_user = "lesid_01"
database_password = "@Ketamine1324!"
database_host = "localhost"
database_port = "5432"

ignore_patterns = [
    "/blo***",
    # Add more patterns here as needed
]
table_name = sanitize_string(extract_root_domain(websites[0]))

min_delay = 1
max_delay = 3
crawl_level = 7
max_urls_to_scrape = 10

previously_scraped_urls = get_scraped_urls_from_database(table_name, database_name, database_user, database_password, database_host, database_port)

HTTPCACHE_ENABLED = False
HTTPCACHE_POLICY = "scrapy.extensions.httpcache.DummyPolicy"
HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"
HTTPCACHE_EXPIRATION_SECS = 86400  # 1 day

def enumerate_subdomains(domain):
    subdomains = []
    try:
        answers = dns.resolver.resolve(f"{domain}", "NS")
        for rdata in answers:
            nsname = str(rdata.target)[:-1]
            subdomains.append(nsname)
    except Exception as e:
        pass
    return subdomains


def is_valid_url(url):
    try:
        # Ignore non-HTML files
        if any(url.lower().endswith(ext) for ext in ignored_extensions):
            return False

        response = requests.head(url, allow_redirects=True, timeout=5)
        content_type = response.headers.get('content-type', '').lower()
        return response.status_code == 200 and ('text/html' in content_type or 'application/xhtml+xml' in content_type)
    except Exception as e:
        return False


subdomains = []

for website in websites:
    domain = urlparse(website).netloc
    subdomains.extend(enumerate_subdomains(domain))

valid_subdomains = [f"https://{subdomain}" for subdomain in subdomains if is_valid_url(f"https://{subdomain}")]
websites.extend(valid_subdomains)

# CrawlerProcess setup
process = CrawlerProcess({
    'USER_AGENT': random_user_agent(),
    'DOWNLOAD_DELAY': random.uniform(min_delay, max_delay),
    'DEPTH_LIMIT': crawl_level,
    'CLOSESPIDER_PAGECOUNT': max_urls_to_scrape if max_urls_to_scrape > 0 else None,
    'CLOSESPIDER_ITEMCOUNT': max_urls_to_scrape if max_urls_to_scrape > 0 else None,
    'HTTPCACHE_ENABLED': HTTPCACHE_ENABLED,
    'HTTPCACHE_POLICY': HTTPCACHE_POLICY,
    'HTTPCACHE_STORAGE': HTTPCACHE_STORAGE,
    'HTTPCACHE_EXPIRATION_SECS': HTTPCACHE_EXPIRATION_SECS,
})

crawler = process.create_crawler(MySpider)
process.crawl(crawler, websites=websites, database_config=(database_name, database_user, database_password, database_host, database_port), ignore_patterns=ignore_patterns, previously_scraped_urls=previously_scraped_urls, max_urls_to_scrape=max_urls_to_scrape, table_name=table_name)
process.start()

# Get the visited URLs from the Scrapy spider
visited_urls = crawler.spider.get_visited_urls()

# Run the dynamic scraper after the Scrapy spider is done
import dynamic_scraper


async def main_async():
    successful_urls = get_scraped_urls_from_database(table_name, database_name, database_user, database_password, database_host, database_port)
    dynamic_df = await dynamic_scraper.scrape_dynamic_content(successful_urls)
    db_operations.update_database_with_dynamic_content(dynamic_df, table_name, (database_name, database_user, database_password, database_host, database_port))

asyncio.run(main_async())