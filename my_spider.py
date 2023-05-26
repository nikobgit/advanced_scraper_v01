import scrapy
from scraper import scrape_page, is_same_domain
from db_operations import save_to_postgres, update_database_with_visited_urls
import pandas as pd
import json
from langdetect import detect
from utilities import clean_text
from urllib.parse import urlparse
from urllib.parse import urlsplit
from utilities import random_user_agent, ignored_extensions


class MySpider(scrapy.Spider):
    name = 'my_spider'

    def closed(self, reason):
        df = self.get_dataframe()
        save_to_postgres(df, self.table_name, *self.database_config, recreate_table=False)

        # Update the database with the visited URLs from the Scrapy spider
        update_database_with_visited_urls(self.visited_urls, self.table_name, self.database_config)

    def should_ignore_url(self, url):
        return any(pattern in url for pattern in self.ignore_patterns)

    def __init__(self, websites, database_config, ignore_patterns, max_urls_to_scrape, previously_scraped_urls, table_name, *args, **kwargs):
        super(MySpider, self).__init__(*args, **kwargs)
        self.start_urls = websites
        self.scraped_data = {}
        self.visited_urls = set(previously_scraped_urls)
        self.database_config = database_config
        self.ignore_patterns = ignore_patterns
        self.previously_scraped_urls = previously_scraped_urls
        self.scraped_items_count = 0
        self.max_urls_to_scrape = max_urls_to_scrape
        self.table_name = table_name


    def parse(self, response):
        main_domain = urlparse(response.url).netloc
        content, code_snippets, links, tables = scrape_page(response.url, main_domain)

        if not self.should_ignore_url(response.url) and response.url not in self.previously_scraped_urls:
            if self.max_urls_to_scrape <= 0 or self.scraped_items_count < self.max_urls_to_scrape:
                self.scraped_data[response.url] = {
                    'content': clean_text(content),
                    'code_snippets': code_snippets,
                    'tables': tables,
                }
                self.scraped_items_count += 1
                self.visited_urls.add(response.url)  # Add the URL to the visited_urls set after scraping

        for link in links:
            if link not in self.visited_urls and is_same_domain(link, response.url):
                yield scrapy.Request(link, callback=self.parse, headers={'User-Agent': random_user_agent()})

        df = self.get_dataframe()
    def get_visited_urls(self):
        return list(self.visited_urls)

    def get_dataframe(self):
        data = []
        for url, value in self.scraped_data.items():
            data.append({
                'url': url,
                'content': value['content'],
                'tables': json.dumps(value['tables']),
                'code_snippets': json.dumps(value['code_snippets']),
                'domain': urlparse(url).netloc
            })

        df = pd.DataFrame(data)

        return df