import pandas as pd
from bs4 import BeautifulSoup
from utilities import ignored_extensions, BLOCK_RESOURCE_TYPES, BLOCK_RESOURCE_NAMES
import logging
logging.basicConfig(level=logging.INFO)
from playwright.async_api import async_playwright
from scraper import extract_tables as extract_tables_soup, extract_code_snippets as extract_code_snippets_soup


async def intercept_route(route, request):
    if request.resource_type.lower() in BLOCK_RESOURCE_TYPES:
        print(f'Blocking background resource {request} blocked type "{request.resource_type}"')
        await route.abort()
    elif any(key in request.url for key in BLOCK_RESOURCE_NAMES):
        print(f"Blocking background resource {request} blocked name {request.url}")
        await route.abort()
    else:
        await route.continue_()

def extract_code_snippets(content):
    soup = BeautifulSoup(content, 'html.parser')
    return extract_code_snippets_soup(soup)

def extract_tables(content):
    soup = BeautifulSoup(content, 'html.parser')
    return extract_tables_soup(soup)

async def dynamic_content_click_modal(page, url):
    dynamic_content_click = ""
    code_snippets_click = []

    try:
        print(f"Going to URL: {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=10000)
        await page.wait_for_function("document.readyState === 'complete'", timeout=10000)
        print("Page fully loaded.")

        print("Looking for buttons...")
        buttons = page.locator('.icon-item')
        button_count = await buttons.count()
        print(f"Found {button_count} button(s).")

        for i in range(button_count):
            print(f"Clicking button #{i+1}...")
            button = buttons.nth(i)
            await button.click()
            print("Button clicked.")

            print("Waiting for modal to appear...")
            modal_locator = page.locator('.modal-dialog')
            await modal_locator.wait_for()
            print("Modal appeared.")

            print("Getting modal text...")
            modal_text = await modal_locator.inner_text()
            dynamic_content_click += modal_text
            print(f"Modal text (first 100 chars): {modal_text[:100]}")

            print("Looking for code snippet in modal...")
            code_snippet_element = modal_locator.locator('pre')
            code_snippet = await code_snippet_element.inner_text()
            code_snippets_click.append(code_snippet)
            print(f"Code snippet found: {code_snippet[:100]}")

            print("Closing modal...")
            await page.keyboard.press('Escape')
            print("Modal closed.")

    except Exception as e:
        logging.error(f"Exception while extracting dynamic click content for {url}: {e}")

    return dynamic_content_click, code_snippets_click



async def extract_data(page):
    dynamic_content_elements = await page.locator("p, blockquote").all()
    dynamic_content = " ".join([await element.inner_text() for element in dynamic_content_elements])
    logging.info(f"Dynamic content excerpt: {dynamic_content[:100]}...")

    tables = extract_tables(await page.content())
    logging.info(f"Number of tables extracted: {len(tables)}")

    code_snippets = extract_code_snippets(await page.content())
    logging.info(f"Number of code snippets extracted: {len(code_snippets)}")

    return dynamic_content, tables, code_snippets


dynamic_urls = set()


async def scroll_and_extract(page):
    global dynamic_urls
    logging.info("Scrolling to bottom of page to load dynamic content...")

    prev_scroll_position = -1
    curr_scroll_position = 0
    previous_url = page.url

    while prev_scroll_position != curr_scroll_position:
        prev_scroll_position = curr_scroll_position
        await page.mouse.wheel(0, page.viewport_size['height'])
        await page.wait_for_load_state("networkidle")

        curr_scroll_position = await page.evaluate("document.documentElement.scrollTop || document.body.scrollTop")
        new_url = page.url

        if new_url != previous_url and new_url not in dynamic_urls:
            dynamic_urls.add(new_url)
            print(f"New URL found while scrolling: {new_url}")
            previous_url = new_url

    logging.info("Extracting dynamic content, tables, and code snippets...")
    return await extract_data(page)

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

            dynamic_urls = set()

            try:
                logging.info(f"Loading {url}...")
                await page.goto(url, wait_until="domcontentloaded", timeout=10000)
                await page.wait_for_function("document.readyState === 'complete'", timeout=10000)

                dynamic_content, tables, code_snippets = await scroll_and_extract(page)
                dynamic_content_click, code_snippets_click = await dynamic_content_click_modal(page, url)
                code_snippets.extend(code_snippets_click)

            except Exception as e:
                logging.error(f"Exception while loading {url}: {e}")
                dynamic_content, tables, code_snippets = "", [], []
                dynamic_content_click, code_snippets_click = "", []

            await page.close()
            await context.close()
            await browser.close()

            return dynamic_content, tables, code_snippets, dynamic_content_click, code_snippets_click, dynamic_urls

    except Exception as e:
        logging.error(f"Exception while extracting data for {url}: {e}")
        return "", [], [], "", []


async def scrape_dynamic_content(urls):
    dynamic_data = []

    for url in urls:
        if any(url.lower().endswith(ext) for ext in ignored_extensions):
            continue

        try:
            dynamic_content, tables, code_snippets, dynamic_content_click, code_snippets_click, dynamic_urls = await extract_dynamic_content(url)
            dynamic_urls = dynamic_urls - {url}  # Remove the original URL from the dynamic URLs set
            dynamic_data.append({
                'url': url,
                'dynamic_content': dynamic_content,
                'dynamic_click': dynamic_content_click,
                'tables': tables,
                'code_snippets': code_snippets,
                'dynamic_urls': list(dynamic_urls),  # Add this line
            })
        except Exception as e:
            logging.error(f"Exception while extracting data for {url}: {e}")
            continue

    df = pd.DataFrame(dynamic_data)
    return df

