# main.py

import asyncio
import os
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from playwright.async_api import async_playwright
from dotenv import load_dotenv
import subprocess
from save_nested import save_cleaned_rows_nested
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware


# Load environment variables
load_dotenv()
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins — change this in production!
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
AGENT_ID = os.getenv("AGENT_ID")
AGENT_NAME = os.getenv("AGENT_NAME")
PASSWORD = os.getenv("PASSWORD")
CHROME_PROFILE_PATH = os.getenv("CHROME_PROFILE_PATH")

PROJECT_DIR = Path(__file__).resolve().parent
CLEAN_DIR = PROJECT_DIR / "cleaned_data"
RAW_DIR = PROJECT_DIR / "hotel_data"
SCREEN_DIR = PROJECT_DIR / "screenshots"

@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI!"}

def load_config():
    with open("august_config_by_city_v2.json", "r", encoding='utf-8') as f:
        return json.load(f)

def validate_date(date_str):
    try:
        datetime.strptime(date_str.strip(), "%d/%m/%Y")
        return True
    except ValueError:
        return False

def price_to_float(price_text: str):
    if not price_text or not isinstance(price_text, str):
        return None
    txt = price_text.replace(",", "")
    digits = "".join(ch for ch in txt if (ch.isdigit() or ch == "." or ch == "-"))
    try:
        return float(digits) if digits not in ("", "-", ".") else None
    except ValueError:
        return None

async def search_city_hotel(page, context, city, hotel_name, checkin, checkout):
    if not (validate_date(checkin) and validate_date(checkout)):
        print(f"❌ Invalid date format for {checkin} - {checkout}")
        return

    await page.goto("https://business.myhotels.sa/HotelSearch", timeout=30000)
    await page.wait_for_selector("#txtCityName", timeout=15000)

    await page.click('#txtCityName')
    await page.fill('#txtCityName', "")
    for char in city:
        await page.type('#txtCityName', char, delay=50)

    await page.wait_for_selector(f"div.autocomplete-suggestion:has-text('{city.lower()}')", timeout=15000)
    suggestions = page.locator("div.autocomplete-suggestion")
    for i in range(await suggestions.count()):
        suggestion_el = suggestions.nth(i)
        text = await suggestion_el.inner_text()
        if f"{city.lower()}, saudi arabia" in text.lower():
            await suggestion_el.click(timeout=5000)
            break

    await page.wait_for_timeout(1500)
    await page.evaluate("document.getElementById('txtCheckinDate').removeAttribute('readonly')")
    await page.evaluate("document.getElementById('txtCheckoutDate').removeAttribute('readonly')")

    await page.fill('#txtCheckinDate', "")
    await page.fill('#txtCheckoutDate', "")
    await page.wait_for_timeout(300)

    await page.fill('#txtCheckinDate', checkin.strip())
    await page.fill('#txtCheckoutDate', checkout.strip())

    await page.click('#btnHotelSearch')
    await page.wait_for_timeout(8000)

    await page.fill('#hotelsearchtext', "")
    await page.wait_for_timeout(300)
    for char in hotel_name:
        await page.type('#hotelsearchtext', char, delay=50)
    await page.keyboard.press('Enter')
    await page.wait_for_timeout(3000)

    hotel_titles = page.locator("span.p_name_title")
    found = False
    for i in range(await hotel_titles.count()):
        title_text = (await hotel_titles.nth(i).inner_text()).strip().lower()
        if hotel_name.strip().lower() in title_text:
            await hotel_titles.nth(i).click()
            found = True
            break

    if not found:
        print(f"❌ Hotel '{hotel_name}' not found in search results for {city}")
        return

    await page.wait_for_timeout(3000)

    pages = context.pages
    if len(pages) < 2:
        print(f"❌ Hotel details tab did not open for {hotel_name}")
        return

    hotel_page = pages[-1]
    await hotel_page.bring_to_front()
    await hotel_page.wait_for_load_state('load')

    safe_hotel = hotel_name.replace(" ", "_")
    safe_date = checkin.replace("/", "-")
    SCREEN_DIR.mkdir(exist_ok=True)
    await hotel_page.screenshot(path=str(SCREEN_DIR / f"{safe_hotel}_{safe_date}.png"), full_page=True)

    table_exists = False
    for _ in range(40):
        count = await hotel_page.evaluate("""
            () => {
                const table = document.querySelector("tbody.mobile_class");
                if (!table) return 0;
                return table.querySelectorAll("tr.color_no").length;
            }
        """)
        if count > 0:
            table_exists = True
            break
        await hotel_page.wait_for_timeout(1000)

    if not table_exists:
        print(f"⚠️ Table not found or empty for {hotel_name} in {city} on {checkin}")
        await hotel_page.close()
        return

    rows = hotel_page.locator("tbody.mobile_class tr.color_no")
    extracted = []

    for i in range(await rows.count()):
        row = rows.nth(i)

        # Room Name
        try:
            room_name_el = row.locator(".room_name")
            if await room_name_el.count() > 0:
                room_name = await room_name_el.inner_text()
            elif extracted:
                room_name = extracted[-1]["R"]
            else:
                room_name = "N/A"
        except:
            room_name = "N/A"

        if room_name == "N/A":
            print("⚠️ Skipping row with missing room name")
            continue

        # Meal Plan
        try:
            meal_plan = await row.locator(".icon_with_text > span:last-child").inner_text()
        except:
            meal_plan = "N/A"

        # Price
        try:
            price = await row.locator("a.total_price .currencytext").first.inner_text()
        except:
            price = "N/A"

        extracted.append({
            "H": hotel_name.strip(),
            "C": city.strip(),
            "D": checkin.strip(),
            "R": room_name.strip(),
            "M": meal_plan.strip(),
            "P": price.strip()
        })

    RAW_DIR.mkdir(exist_ok=True)
    safe_filename = f"{safe_hotel}_{safe_date}.json"
    with open(RAW_DIR / safe_filename, "w", encoding="utf-8") as f:
        json.dump(extracted, f, ensure_ascii=False, indent=2)

    print(f"✅ Extracted {len(extracted)} rows for {hotel_name} in {city} ({checkin} - {checkout})")
    print(f"💾 Saved to hotel_data/{safe_filename}")
    await hotel_page.close()


async def run():
    config = load_config()
    MAX_RETRIES = 2

    async with async_playwright() as p:
        browser = await p.chromium.launch_persistent_context(
            CHROME_PROFILE_PATH,
            headless=False,
            channel="chrome",
            args=["--disable-popup-blocking", "--disable-notifications"]
        )
        context = browser
        page = await browser.new_page()
        await page.goto("https://business.myhotels.sa/")
        await page.wait_for_timeout(5000)

        # Login
        if await page.is_visible("#txtSignInAgentcode"):
            await page.fill('#txtSignInAgentcode', AGENT_ID)
            await page.fill('#txtSignInUsername', AGENT_NAME)
            await page.fill('#txtSignInPassword', PASSWORD)
            await page.check('#chkRememberMe')
            await page.click('#btnLogin')

        # OTP (if needed)
        try:
            await page.wait_for_selector('#txtOtpId', timeout=10000)
            print("🔐 OTP required. Please check your email or phone.")
            otp = input("🔑 Enter OTP here: ")
            await page.fill('#txtOtpId', otp)
            await page.click('#btnLogin1')
            await page.wait_for_timeout(5000)
        except:
            print("✅ No OTP requested, continuing login.")

        # Iterate config
        for city, hotels in config.items():
            if city == "dates":
                continue
            for hotel in hotels:
                for checkin, checkout in config["dates"]:
                    for attempt in range(MAX_RETRIES):
                        try:
                            print(f"🔍 Searching {hotel} in {city} from {checkin} to {checkout}")
                            await search_city_hotel(page, context, city, hotel, checkin, checkout)
                            break
                        except Exception as e:
                            print(f"❌ Attempt {attempt+1} failed: {e}")
                            if attempt == MAX_RETRIES - 1:
                                print("⚠️ Skipping after multiple failures.")

        await browser.close()

    # Run cleaning script (unchanged)
    print("\n⚙️ Running cleaner...")
    import sys
    CLEAN_DIR.mkdir(exist_ok=True)
    result = subprocess.run([sys.executable, "clean_with_openai.py"])
    if result.returncode != 0:
        print("❌ Cleaner failed. Aborting Firestore save.")
        return

    # -------- NEW: Load ALL cleaned files, parse hotel/date from filename, map city, then save --------

    def build_hotel_to_city_map(cfg: Dict[str, Any]) -> Dict[str, str]:
        m = {}
        for c, hotels in cfg.items():
            if c == "dates":
                continue
            for h in hotels:
                m[h.strip().lower()] = c
        return m

    def parse_hotel_date_from_filename(p: Path):
        # Zaha_Al_Munawara_Hotel_31-08-2025.json -> ("Zaha Al Munawara Hotel", "31-08-2025")
        stem = p.stem
        if "_" in stem:
            *name_parts, date_part = stem.split("_")
            hotel_from_file = " ".join(name_parts).replace("-", " ").strip()
            return hotel_from_file, date_part
        return stem.replace("_", " ").strip(), None

    hotel_to_city = build_hotel_to_city_map(config)
    files = sorted(CLEAN_DIR.glob("*.json"))
    if not files:
        print("❌ No cleaned JSON files found in cleaned_data/")
        return

    normalized: List[Dict[str, Any]] = []
    for fp in files:
        hotel_from_file, date_from_file = parse_hotel_date_from_filename(fp)
        try:
            rows = json.loads(fp.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"⚠️ Skipping {fp.name}: {e}")
            continue

        for r in rows:
            # accept many possible keys from cleaner
            room = r.get("normalized_room_type")
            meal = r.get("normalized_meal")
            price = r.get("P")

            # skip 'ignore' rows
            if isinstance(room, str) and room.strip().lower() == "ignore":
                continue

            hotel = r.get("hotel") or r.get("H") or hotel_from_file
            city_val = r.get("city") or r.get("C") or hotel_to_city.get(hotel.strip().lower())
            date_val = r.get("date") or r.get("D") or date_from_file  # supports DD-MM-YYYY later

            if not (city_val and hotel and date_val and room):
                continue

            if isinstance(price, str):
                price = price_to_float(price)

            normalized.append({
                "city": str(city_val),
                "hotel": str(hotel),
                "date": str(date_val),            # save_nested converts to Timestamp
                "room_name": str(room),
                "meal_plan": str(meal) if meal else "",
                "price": float(price) if price is not None else None,
                "currency": r.get("currency", "SAR"),
                "available": bool(r.get("available", True)),
                "source": r.get("source", "myhotels.sa"),
                "scraped_at": r.get("scraped_at"),
            })

            class LoginRequest(BaseModel):
                agentId: str
                username: str
                password: str
                otp: str = ""  # Optional by default

            @app.post("/login")
            async def login(data: LoginRequest):
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    context = await browser.new_context()
                    page = await context.new_page()

                    await page.goto("https://business.myhotels.sa/")

                    # Fill login fields
                    await page.fill("#AgencyCode", data.agentId)
                    await page.fill("#UserName", data.username)
                    await page.fill("#Password", data.password)
                    await page.click("#LoginButton")

                    # Check if OTP is requested
                    try:
                        otp_input = await page.wait_for_selector("#OtpCode", timeout=3000)
                        if data.otp == "":
                            await browser.close()
                            return {
                                "requiresOtp": True,
                                "message": "OTP required. Please enter it."
                            }

                        await otp_input.fill(data.otp)
                        await page.click("#OtpSubmit")

                    except:
                        # No OTP requested, continue
                        pass

                    # After OTP, check if redirected to dashboard
                    if "dashboard" in page.url.lower():
                        await browser.close()
                        return {"success": True, "message": "Login successful."}
                    else:
                        await browser.close()
                        return {"success": False, "message": "Login failed. Check credentials or OTP."}

    if not normalized:
        print("⚠️ No valid cleaned rows to save after normalization.")
        return

    summary = save_cleaned_rows_nested(normalized)
    print(f"✅ Saved {summary.get('written', 0)} cleaned room docs to Firestore.")

if __name__ == '__main__':
    asyncio.run(run())
