import pytest
from playwright.async_api import async_playwright
from dynamic_scraper import interact_with_icon_items

@pytest.mark.asyncio
async def test_interact_with_icon_items():
    url = "https://platform.openai.com/docs/plugins/examples"

    # Launch the browser and open a new page
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

        # Go to the specified URL
        await page.goto(url, wait_until="domcontentloaded", timeout=10000)
        await page.wait_for_function("document.readyState === 'complete'", timeout=10000)

        # Call the interact_with_icon_items function
        dynamic_content, tables, code_snippets = await interact_with_icon_items(page)

        # Close the browser
        await page.close()
        await context.close()
        await browser.close()

    # Print the results
    print(f"Dynamic content: {dynamic_content}")
    print(f"Tables: {tables}")
    print(f"Code snippets: {code_snippets}")

    # Check if the function is working as expected
    assert dynamic_content != ""
    assert len(tables) > 0
    assert len(code_snippets) > 0