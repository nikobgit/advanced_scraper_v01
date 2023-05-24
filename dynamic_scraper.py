import asyncio
from db_operations import save_to_postgres
import pandas as pd
from bs4 import BeautifulSoup
import psycopg2
import json
from utilities import ignored_extensions
import logging
logging.basicConfig(level=logging.INFO)
from playwright.async_api import async_playwright, TimeoutError
import db_operations

def playwright_logger(name: str, severity: str, message: str, args: list, hints: dict):
    logger = logging.getLogger(name)
    log_method = getattr(logger, severity.lower())
    log_method(message.format(*args))

logging.getLogger("playwright").setLevel(logging.DEBUG)
logging.getLogger("playwright").addHandler(logging.StreamHandler())


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


async def intercept_route(route, request):
    if request.resource_type.lower() in BLOCK_RESOURCE_TYPES:
        print(f'Blocking background resource {request} blocked type "{request.resource_type}"')
        await route.abort()
    elif any(key in request.url for key in BLOCK_RESOURCE_NAMES):
        print(f"Blocking background resource {request} blocked name {request.url}")
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


async def extract_data(page):
    dynamic_content_elements = await page.query_selector_all("p, blockquote")
    dynamic_content = " ".join([await element.inner_text() for element in dynamic_content_elements])
    logging.info(f"Dynamic content excerpt: {dynamic_content[:100]}...")

    tables = extract_tables(await page.content())
    logging.info(f"Number of tables extracted: {len(tables)}")

    code_snippets = extract_code_snippets(await page.content())
    logging.info(f"Number of code snippets extracted: {len(code_snippets)}")

    return dynamic_content, tables, code_snippets


async def scroll_and_extract(page):
    logging.info("Scrolling to bottom of page to load dynamic content...")
    await page.mouse.wheel(0, page.viewport_size['height'])
    await page.wait_for_load_state("networkidle")

    logging.info("Extracting dynamic content, tables, and code snippets...")
    return await extract_data(page)


async def interact_with_elements(page):
    timeout_value = 3000
    dynamic_content_click, tables_click, code_snippets_click = "", [], []

    try:
        await page.waitForSelector(".icon-item", timeout=timeout_value)
    except TimeoutError:
        logging.warning("Timed out while waiting for expandable elements or modals. Skipping interaction with elements.")
        return dynamic_content_click, tables_click, code_snippets_click

    except Exception as e:
        logging.warning(f"Exception while interacting with elements: {e}")

    try:
        icon_items = await page.query_selector_all(".icon-item")

        for icon_item in icon_items:
            await icon_item.click()
            await page.waitForSelector("div.modal-dialog-container", timeout=timeout_value)

            # Extract content from opened modal
            prompt_elem = await page.query_selector("div.prompt")
            prompt_text = await prompt_elem.inner_text() if prompt_elem else ""

            details_header_elem = await page.query_selector("#example-modal > div.modal-dialog-container > div > div.details-header")
            details_header_text = await details_header_elem.inner_text() if details_header_elem else ""

            details_description_elem = await page.query_selector("#example-modal > div.modal-dialog-container > div > div.details-description")
            details_description_text = await details_description_elem.inner_text() if details_description_elem else ""

            dynamic_content_click += f"{prompt_text} {details_header_text} {details_description_text} "

            # Extract code snippets
            code_sample_elem = await page.query_selector("#example-modal > div.modal-dialog-container > div > div.example-details-content > div.left-panel > div.api-request > div.code-sample > div.code-sample-header > div.code-sample-copy")
            if code_sample_elem:
                await code_sample_elem.click()
                code_sample_text = await page.evaluate("() => window.getSelection().toString()")
                code_snippets_click.append(code_sample_text)

            await page.keyboard.press("Escape")
            await page.waitForSelector("body", timeout=timeout_value)

    except Exception as e:
        logging.warning(f"Exception while interacting with elements: {e}")

    return dynamic_content_click, tables_click, code_snippets_click


async def extract_dynamic_content(url):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            context = await browser.new_context(
                java_script_enabled=True,
                accept_downloads=False,
                ignore_https_errors=True,
                bypass_csp=True,
                viewport={"width": 1920, "height": 1080}
            )

            page = await context.new_page()

            await page.route("**/*", intercept_route)

            try:
                logging.info(f"Loading {url}...")
                await page.goto(url, wait_until="domcontentloaded", timeout=10000)
                await page.wait_for_function("document.readyState === 'complete'", timeout=10000)

                dynamic_content_scroll, tables_scroll, code_snippets_scroll = await scroll_and_extract(page)

                try:
                    dynamic_content_click, tables_click, code_snippets_click = await interact_with_elements(page)
                except Exception as e:
                    logging.warning(f"Exception while interacting with elements on {url}: {e}")
                    dynamic_content_click, tables_click, code_snippets_click = "", [], []

            except Exception as e:
                logging.error(f"Exception while loading {url}: {e}")
                dynamic_content_scroll, tables_scroll, code_snippets_scroll = "", [], []
                dynamic_content_click, tables_click, code_snippets_click = "", [], []

            await page.close()
            await context.close()
            await browser.close()

            return (dynamic_content_scroll, tables_scroll, code_snippets_scroll,
                    dynamic_content_click, tables_click, code_snippets_click)
    except Exception as e:
        logging.error(f"Exception while extracting data for {url}: {e}")
        return "", [], [], "", [], []


async def scrape_dynamic_content(urls):
    dynamic_data = []

    for url in urls:
        # Check if URL has any ignored extensions
        if any(url.lower().endswith(ext) for ext in ignored_extensions):
            continue  # If the URL has an ignored extension, skip it

        try:
            (dynamic_content_scroll, tables_scroll, code_snippets_scroll,
             dynamic_content_click, tables_click, code_snippets_click) = await extract_dynamic_content(url)
            dynamic_data.append({
                'url': url,
                'dynamic_content_scroll': dynamic_content_scroll,
                'tables_scroll': tables_scroll,
                'code_snippets_scroll': code_snippets_scroll,
                'dynamic_content_click': dynamic_content_click,
                'tables_click': tables_click,
                'code_snippets_click': code_snippets_click,
            })
        except Exception as e:
            logging.error(f"Exception while extracting data for {url}: {e}")
            continue

    df = pd.DataFrame(dynamic_data)
    return df

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