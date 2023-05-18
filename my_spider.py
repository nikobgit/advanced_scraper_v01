import scrapy
from scraper import scrape_page, is_same_domain
from db_operations import save_to_postgres
import pandas as pd
import json
from langdetect import detect
from utilities import clean_text
from urllib.parse import urlparse
from urllib.parse import urlsplit
from utilities import random_user_agent, ignored_extensions
from utilities import is_visited



class MySpider(scrapy.Spider):
    name = 'my_spider'

    def closed(self, reason):
        df = self.get_dataframe()
        table_name = 'scraped_data'
        save_to_postgres(df, table_name, *self.database_config, recreate_table=True)

    def should_ignore_url(self, url):
        return any(pattern in url for pattern in self.ignore_patterns)

    def __init__(self, websites, csv_file_name, database_config, ignore_patterns, visited_urls, *args, **kwargs):
        super(MySpider, self).__init__(*args, **kwargs)
        self.start_urls = websites
        self.scraped_data = {}
        self.visited_urls = set(visited_urls)  # Update this line
        self.csv_file_name = csv_file_name
        self.database_config = database_config
        self.ignore_patterns = ignore_patterns

    def parse(self, response):
        if is_visited(response.url, self.visited_urls, scraper_type="scrapy"):  # Add scraper_type parameter
            return

    def parse(self, response):
        if response.url in self.visited_urls:
            return

        # Ignore media files
        media_extensions = ignored_extensions
        self.visited_urls.add(response.url)
        main_domain = urlparse(response.url).netloc
        content, code_snippets, links, tables = scrape_page(response.url, main_domain)

        # Save scraped data only if the URL does not match any ignore patterns
        if not self.should_ignore_url(response.url):
            self.scraped_data[response.url] = {
                'content': clean_text(content),
                'code_snippets': code_snippets,
                'tables': tables,
            }

        for link in links:
            if link not in self.visited_urls and is_same_domain(link, response.url):
                self.start_urls.append(link)
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