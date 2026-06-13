"""
talabat_scraper.py
──────────────────
Scrapes restaurant listings and menu data from Talabat for Egyptian cities.

Outputs (saved to scraping_scripts/Data_files/):
    alex_{date}_talabat.csv    ← restaurant summaries for Alexandria
    cairo_{date}_talabat.csv   ← restaurant summaries for Cairo
"""

import re
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────
OUTPUT_DIR = "/opt/airflow/scraping_scripts/Data_files"

CITY_URLS = {
    "cairo": "https://www.talabat.com/egypt/restaurants/7766/downtown-magra-el-ouyon",
    "alex":  "https://www.talabat.com/egypt/restaurants/7177/el-hadara",
}


# =============================================================================
#   DRIVER SETUP
# =============================================================================
def create_driver() -> webdriver.Remote:
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    return webdriver.Remote(command_executor="http://selenium-chrome:4444/wd/hub", options=options)


# =============================================================================
#   SCRAPING LOGIC
# =============================================================================
def get_restaurant_links(driver: webdriver.Remote, base_url: str, max_pages: int = 56) -> list:
    """Collect all restaurant URLs from the city listing pages."""
    all_links = []

    # for page_num in range(1, max_pages + 1):
    for page_num in range(1,3):
        try:
            url = base_url if page_num == 1 else f"{base_url}?page={page_num}"
            print(f"  📄 Scraping page {page_num}...")
            driver.get(url)
            time.sleep(5)

            soup = BeautifulSoup(driver.page_source, "html.parser")
            links = soup.find_all("a", {"data-testid": "restaurant-a"})

            for link in links:
                href = link.get("href")
                if href:
                    full_url = f"https://www.talabat.com{href}" if href.startswith("/") else href
                    all_links.append(full_url)

            print(f"     Found {len(links)} restaurants on page {page_num}")

        except Exception as e:
            print(f"  ⚠️  Error on page {page_num}: {e}")
            continue

    return list(set(all_links))  # deduplicate


def scrape_restaurant(driver: webdriver.Remote, restaurant_url: str) -> dict:
    """Scrape a single restaurant page and return its summary data."""
    driver.get(restaurant_url)
    time.sleep(5)

    # Scroll to load all menu items
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    soup = BeautifulSoup(driver.page_source, "html.parser")

    # ── Restaurant name & location ────────────────────────────────────────────
    title_elem = soup.find("h1", {"data-testid": "restaurant-title"})
    if title_elem:
        restaurant_name = title_elem.contents[0].strip() if title_elem.contents else "Unknown"
        location_elem = title_elem.find("small")
        if location_elem:
            location_text = location_elem.get_text(strip=True)
            location = location_text.replace("in ", "", 1) if location_text.startswith("in ") else location_text
        else:
            location = "Location not found"
    else:
        h1 = soup.find("h1")
        restaurant_name = h1.get_text(strip=True) if h1 else "Unknown"
        location = "Location not found"

    # ── Cuisines ──────────────────────────────────────────────────────────────
    cuisines_elem = soup.find("p", {"data-testid": "cuisines"})
    cuisines = cuisines_elem.get_text(strip=True) if cuisines_elem else "Cuisines not found"

    # ── Menu items & prices ───────────────────────────────────────────────────
    items = soup.find_all("div", {"class": lambda c: c and "d-flex justify-content-between py-2 clickable" in c})
    print(f"     Found {len(items)} menu items")

    restaurant_prices = []
    for item in items:
        price_elem = item.find("span", {"class": "currency"})
        if price_elem:
            price_numbers = re.findall(r'[\d.]+', price_elem.get_text(strip=True))
            if price_numbers:
                restaurant_prices.append(float(price_numbers[0]))

    return {
        "Restaurant":   restaurant_name,
        "Location":     location,
        "Cuisines":     cuisines,
        "URL":          restaurant_url,
        "Total_Items":  len(items),
        "Prices_List":  restaurant_prices,
        "Min_Price":    min(restaurant_prices) if restaurant_prices else None,
        "Max_Price":    max(restaurant_prices) if restaurant_prices else None,
        "Avg_Price":    sum(restaurant_prices) / len(restaurant_prices) if restaurant_prices else None,
    }


def run_city(city_name: str, base_url: str, today: str) -> None:
    """Run the full scraping pipeline for one city and save output CSV."""
    print(f"\n{'='*50}")
    print(f"🍽️  Scraping Talabat: {city_name.upper()}")
    print(f"{'='*50}")

    driver = create_driver()
    all_restaurant_info = []

    try:
        restaurant_links = get_restaurant_links(driver, base_url)
        print(f"\n✅ Total unique restaurants found: {len(restaurant_links)}")

        for idx, url in enumerate(restaurant_links, 1):
            print(f"\n  🔍 [{idx}/{len(restaurant_links)}] {url}")
            try:
                info = scrape_restaurant(driver, url)
                all_restaurant_info.append(info)
                print(f"     ✔ {info['Restaurant']} | {info['Total_Items']} items")
            except Exception as e:
                print(f"     ⚠️  Error: {e}")
                continue
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    if all_restaurant_info:
        df = pd.DataFrame(all_restaurant_info)
        file_name = f"{city_name}_{today}_talabat.csv"
        output_path = f"{OUTPUT_DIR}/{file_name}"
        df.to_csv(output_path, index=False)
        print(f"\n💾 Saved {len(df)} restaurants → {file_name}")
    else:
        print(f"\n⚠️  No data scraped for {city_name}")


# =============================================================================
#   ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    today = datetime.now().strftime("%Y-%m-%d")

    for city_name, base_url in CITY_URLS.items():
        run_city(city_name, base_url, today)

    print("\n✅ Talabat scraping complete.")