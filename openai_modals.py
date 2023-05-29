from playwright.sync_api import Playwright, sync_playwright, expect

def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto("https://platform.openai.com/examples")

    # Find all buttons with the .icon-item selector
    buttons = page.locator('.icon-item')

    # Get the count of buttons
    button_count = buttons.count()

    # Loop through each button and scrape the content of the modals that appear
    for i in range(button_count):
        # Click the button to open the modal
        button = buttons.nth(i)
        button.click()

        # Wait for the modal to appear
        modal_locator = page.locator('.modal-dialog')
        modal_locator.wait_for()

        # Scrape text from the modal
        modal_text = modal_locator.inner_text()
        print("Modal text:", modal_text)

        # Find the code snippet element within the modal (assuming it's inside a <pre> element)
        code_snippet_element = modal_locator.locator('pre')
        code_snippet = code_snippet_element.inner_text()
        print("Code Snippet:", code_snippet)

        # Close the modal using the "Escape" key
        page.keyboard.press('Escape')

    context.close()
    browser.close()

with sync_playwright() as playwright:
    run(playwright)