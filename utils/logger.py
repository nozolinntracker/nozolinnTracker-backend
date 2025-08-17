from playwright.sync_api import sync_playwright
import pandas as pd

def get_myhotels_data():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://business.myhotels.sa")

        # Login
        page.fill("input[name='username']", "your_username")
        page.fill("input[name='password']", "your_password")
        page.click("button[type='submit']")

        # Wait for the dashboard
        page.wait_for_selector("your-hotels-table-selector")

        # Scrape data (room name, price, availability)
        # Save to dataframe
        df = pd.DataFrame([
            {"hotel": "X", "room": "Standard", "price": 400, "available": True},
            ...
        ])

        browser.close()
        return df
