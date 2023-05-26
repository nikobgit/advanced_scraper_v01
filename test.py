import pytest
import asyncio
import logging
from playwright.async_api import async_playwright
from dynamic_scraper import interact_with_icon_items

# Enable logging
logging.basicConfig(level=logging.INFO)

# URL to test
URL = "https://platform.openai.com/docs/plugins/examples"


@pytest.mark.asyncio
async def test_extract_dynamic_content():
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
        await page.goto(URL, wait_until="domcontentloaded", timeout=10000)
        await page.wait_for_function("document.readyState === 'complete'", timeout=10000)

        tables_click, code_snippets_click, dynamic_content_click = await interact_with_icon_items(page)

        print("\nDynamic content after interaction:")
        print("Dynamic content click:")
        print(dynamic_content_click)

        print("Tables click:")
        for table in tables_click:
            print(table)

        print("Code snippets click:")
        for code_snippet in code_snippets_click:
            print(code_snippet)

        await page.close()
        await context.close()
        await browser.close()

