import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
import time
import random
import json
import nltk
from utilities import random_user_agent
# Download necessary NLTK resources
nltk.download('stopwords')
nltk.download('wordnet')
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from utilities import is_visited

def get_root_domain(url):
    parsed_url = urlparse(url)
    if parsed_url.hostname is not None:
        url_domain_parts = parsed_url.hostname.split('.')
        url_root_domain = '.'.join(url_domain_parts[-2:])
        return url_root_domain
    else:
        return None


def is_same_root_domain(domain1, domain2):
    domain1_parts = domain1.split('.')
    domain2_parts = domain2.split('.')
    return domain1_parts[-2:] == domain2_parts[-2:]


def extract_links(url, soup, main_domain):
    links = set()
    main_domain_parts = main_domain.split('.')
    main_root_domain = '.'.join(main_domain_parts[-2:])
    subdomain_root_domain = '.'.join(main_domain_parts)

    # Extract links from a tags
    for a_tag in soup.find_all('a', href=True):
        link = a_tag['href']
        if not link.startswith('http'):
            link = urljoin(url, link)

        parts = link.split('/')
        if len(parts) > 2:
            domain = parts[2]
            if domain.endswith(main_root_domain) or domain.endswith(subdomain_root_domain):
                links.add(link)

    # Extract links from clickable elements
    clickable_elements = soup.select('[role="button"], button, .icon-item, .clickable, [data-clickable], a[role="link"], .link-item, .btn, .button')
    for element in clickable_elements:
        link = element.get('href') or element.get('data-href')
        if link:
            if not link.startswith('http'):
                link = urljoin(url, link)

            parts = link.split('/')
            if len(parts) > 2:
                domain = parts[2]
                if domain.endswith(main_root_domain) or domain.endswith(subdomain_root_domain):
                    links.add(link)

    return links


def extract_tables(soup):
    tables = []
    for table in soup.find_all("table"):
        table_data = []
        for row in table.find_all("tr"):
            row_data = [cell.text for cell in row.find_all(["th", "td"])]
            table_data.append(row_data)
        tables.append(table_data)
    return tables


def extract_code_snippets(soup):
    code_snippets = [code.text for code in soup.find_all("code")]
    return code_snippets


def scrape_page(url, main_domain):
    headers = {'User-Agent': random_user_agent()}  # Fixed this line
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    response.encoding = 'utf-8'  # Set the encoding to 'utf-8' directly

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract different elements
    links = extract_links(url, soup, main_domain)
    tables = extract_tables(soup)
    code_snippets = extract_code_snippets(soup)

    # Extract the main content text
    content = ' '.join([p_tag.text for p_tag in soup.find_all('p')])

    return content, code_snippets, links, tables,


def is_same_domain(url, main_domain):
    parsed_main_domain = urlparse(main_domain)
    main_domain_parts = parsed_main_domain.hostname.split('.')
    main_root_domain = '.'.join(main_domain_parts[-2:])

    parsed_url = urlparse(url)
    url_domain_parts = parsed_url.hostname.split('.')
    url_root_domain = '.'.join(url_domain_parts[-2:])

    # Check if root domains are the same or if the main domain is a subdomain of the url domain
    return main_root_domain == url_root_domain or main_domain.endswith(f".{url_root_domain}")

def clean_text(text):
    text = text.lower()
    lemmatizer = WordNetLemmatizer()
    stop_words = set(stopwords.words('english'))
    words = re.findall(r'\w+', text)
    words = [lemmatizer.lemmatize(word) for word in words if word not in stop_words]
    text = ' '.join(words)
    return text