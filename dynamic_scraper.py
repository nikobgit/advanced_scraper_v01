import asyncio
from playwright.async_api import async_playwright
from db_operations import save_to_postgres
import pandas as pd
from bs4 import BeautifulSoup
import psycopg2
import json
from utilities import ignored_extensions
from utilities import is_visited

BLOCK_RESOURCE_TYPES = [
    'beacon',
    'csp_report',
    'font',
    'image',
    'imageset',
    'media',
    'object',
    'texttrack',
]

BLOCK_RESOURCE_NAMES = [
    'adzerk',
    'analytics',
    'cdn.api.twitter',
    'doubleclick',
    'exelator',
    'facebook',
    'fontawesome',
    'google',
    'google-analytics',
    'googletagmanager',
]

async def scrape_dynamic_content(urls, visited_urls):  # Add visited_urls parameter
    dynamic_data = []

    for url in urls:
        # Check if URL has any ignored extensions or if it has already been visited
        if any(url.lower().endswith(ext) for ext in ignored_extensions) or is_visited(url, visited_urls):  # Update this line
            continue


async def intercept_route(route):
    if route.request.resource_type.lower() in BLOCK_RESOURCE_TYPES:
        print(f'Blocking background resource {route.request} blocked type "{route.request.resource_type}"')
        await route.abort()
    elif any(key in route.request.url for key in BLOCK_RESOURCE_NAMES):
        print(f"Blocking background resource {route.request} blocked name {route.request.url}")
        await route.abort()
    else:
        await route.continue_()


def extract_tables(content):
    soup = BeautifulSoup(content, 'html.parser')
    tables = []
    for table in soup.find_all("table"):
        table_data = []
        for row in table.find_all("tr"):
            row_data = [cell.text for cell in row.find_all(["th", "td"])]
            table_data.append(row_data)
        tables.append(table_data)
    return tables


def extract_code_snippets(content):
    soup = BeautifulSoup(content, 'html.parser')
    code_snippets = [code.text for code in soup.find_all("code")]
    return code_snippets


def extract_paragraphs_and_blockquotes(content):
    soup = BeautifulSoup(content, 'html.parser')
    elements = soup.find_all(['p', 'blockquote'])
    return " ".join([element.text for element in elements])


async def extract_dynamic_content(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch()

        # Create a new context with the suggested configuration
        context = await browser.new_context(
            java_script_enabled=True,
            accept_downloads=False,
            ignore_https_errors=True,
            bypass_csp=True,
            viewport={"width": 1920, "height": 1080},)

        page = await context.new_page()

        # Add the following line to enable intercepting for this page and await it
        await page.route("**/*", intercept_route)

        try:
            # Navigate to the URL without waiting for the load to complete
            await page.goto(url, wait_until="domcontentloaded", timeout=10000)

            # Wait for the document to be in a ready state
            await page.wait_for_function("document.readyState === 'complete'", timeout=10000)

            # Scroll down to load dynamic content and wait for network idle
            for _ in range(10):  # Adjust the range based on how much you want to scroll
                await page.mouse.wheel(0, 300)  # Scroll 300 pixels vertically
                await asyncio.sleep(0.5)  # Wait for 500ms between each scroll action
            await page.wait_for_load_state("networkidle")

            # Click on the expandable elements
            expandable_elements = await page.query_selector_all(".expn-title")
            for element in expandable_elements:
                await element.click()
                await asyncio.sleep(1)  # Wait for 1s to allow content to load or animations to complete

            # Click on buttons to open modals
            modal_buttons = await page.query_selector_all(".icon-item")
            for button in modal_buttons:
                await button.click()
                await asyncio.sleep(1)  # Wait for 1s to allow the modal to open

                # Scroll within the modal to reveal more content
                modal = await page.query_selector("#example-modal > div.modal-dialog-container > div")
                if modal:
                    for _ in range(5):  # Adjust the range based on how much you want to scroll within the modal
                        await page.keyboard.press("PageDown")
                        await asyncio.sleep(0.5)  # Wait for 500ms between each scroll action

                # Close the modal programmatically by clicking outside the modal or pressing the Escape key
                await page.keyboard.press("Escape")
                await asyncio.sleep(1)  # Wait for 1s to allow the modal to close

            # Extract dynamic content, tables, and code snippets from the page
            html_content = await page.content()
            dynamic_content = extract_paragraphs_and_blockquotes(html_content)
            tables = extract_tables(html_content)
            code_snippets = extract_code_snippets(html_content)

        except Exception as e:
            print(f"Exception while loading {url}: {e}")
            dynamic_content, tables, code_snippets = "", [], []

        # Close the page and context to free resources
        await page.close()
        await context.close()
        await browser.close()

        return dynamic_content, tables, code_snippets




async def scrape_dynamic_content(urls, visited_urls):  # Add visited_urls parameter
    dynamic_data = []

    for url in urls:
        # Check if URL has any ignored extensions or if it has already been visited
        if any(url.lower().endswith(ext) for ext in ignored_extensions) or is_visited(url, visited_urls, scraper_type="playwright"):  # Update this line
            continue

        dynamic_content, tables, code_snippets = await extract_dynamic_content(url)
        dynamic_data.append({
            'url': url,
            'dynamic_content': dynamic_content,
            'tables': tables,
            'code_snippets': code_snippets,
        })

    df = pd.DataFrame(dynamic_data)
    return df


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
        update_query = """
                UPDATE visited_urls
                SET scraped = TRUE,
                    attempts = attempts + 1
                WHERE url = %s;
            """
        if not row['dynamic_content']:
            update_query = """
                    UPDATE visited_urls
                    SET attempts = attempts + 1
                    WHERE url = %s;
                """

        cursor.execute(update_query, (row['url'],))
        conn.commit()

    cursor.close()
    conn.close()