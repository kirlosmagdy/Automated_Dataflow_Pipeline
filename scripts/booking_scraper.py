"""
booking_scraper.py
──────────────────
Scrapes hotel listings and room details from Booking.com for Egyptian cities.
Connects to the selenium-chrome Docker container via Selenium Grid.

Outputs (saved to /opt/airflow/scraping_scripts/Data_files/):
    {City}_{date}_booking.csv        ← one row per hotel
    {City}_{date}_booking_rooms.csv  ← one row per room type per hotel
"""

import re
import time
from urllib.parse import quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime

import pandas as pd
from geopy.distance import geodesic
from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ── Config ────────────────────────────────────────────────────────────────────
OUTPUT_DIR     = "/opt/airflow/scraping_scripts/Data_files"
SELENIUM_URL   = "http://selenium-chrome:4444/wd/hub"   # Selenium Grid inside Docker

CITIES         = ["Alexandria", "Cairo", "Luxor", "Sharm El Sheikh"]
CHECKIN        = "2026-07-10"
CHECKOUT       = "2026-07-15"
LIMIT          = 25


# =============================================================================
#   DRIVER SETUP
#   FIX: Added retry loop — if the Selenium Grid is busy and returns a timeout,
#        we wait and try again instead of crashing immediately.
# =============================================================================
def create_driver(retries: int = 5, delay: int = 15) -> webdriver.Remote:
    """
    Connect to the Selenium Grid (selenium-chrome container).

    Args:
        retries: how many times to retry if session creation fails
        delay:   seconds to wait between retries

    Returns:
        A connected Remote WebDriver instance.
    """
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=en-US")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_experimental_option(
        "prefs",
        {
            "profile.managed_default_content_settings.images": 2,
            "intl.accept_languages": "en-US,en",
        },
    )
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.page_load_strategy = "eager"

    last_exception = None
    for attempt in range(1, retries + 1):
        try:
            print(f"🔌 Connecting to Selenium Grid (attempt {attempt}/{retries})...")
            driver = webdriver.Remote(
                command_executor=SELENIUM_URL,
                options=options,
                keep_alive=True,   # keeps the HTTP connection alive between commands
            )
            driver.execute_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            print("✅ Connected to Selenium Grid.")
            return driver
        except Exception as e:
            last_exception = e
            print(f"⚠️  Session creation failed (attempt {attempt}): {e}")
            if attempt < retries:
                print(f"   Retrying in {delay}s...")
                time.sleep(delay)

    raise RuntimeError(
        f"Could not create a Selenium session after {retries} attempts. "
        f"Last error: {last_exception}"
    )


def _build_search_url(destination: str, checkin: str, checkout: str) -> str:
    q = quote_plus(destination)
    return (
        "https://www.booking.com/searchresults.html"
        f"?ss={q}&checkin={checkin}&checkout={checkout}"
        "&group_adults=2&no_rooms=1&group_children=0"
        "&selected_currency=USD&lang=en-us&soz=1"
    )


# =============================================================================
#   HELPERS
# =============================================================================
def force_click(driver: webdriver.Remote, element) -> None:
    driver.execute_script(
        """
        var rect = arguments[0].getBoundingClientRect();
        var x = rect.left + (rect.width / 2);
        var y = rect.top + (rect.height / 2);
        var el = arguments[0];
        ['mousedown','mouseup','click'].forEach(function(evt){
            var e = new MouseEvent(evt, {
                bubbles: true, cancelable: true, view: window,
                clientX: x, clientY: y
            });
            el.dispatchEvent(e);
        });
    """,
        element,
    )


def _scroll_results_container(driver: webdriver.Remote) -> None:
    containers = driver.find_elements(By.XPATH, '//div[@data-testid="search-results"]')
    if containers:
        driver.execute_script("arguments[0].scrollBy(0, 1600);", containers[0])
    else:
        driver.execute_script("window.scrollBy(0, 1600);")


def _dedupe_keep_order(values: List[str]) -> List[str]:
    return list(dict.fromkeys([v.strip() for v in values if v and v.strip()]))


def _clean_text(txt: str) -> str:
    return re.sub(r"\s+", " ", txt or "").strip()


def _dismiss_common_popups(driver: webdriver.Remote) -> None:
    close_xpaths = [
        '//button[@aria-label="Dismiss sign-in info."]',
        '//button[@aria-label="Close"]',
        '//button[contains(@data-testid,"close")]',
        '//button[.//span[contains(normalize-space(),"Dismiss")]]',
        '//button[.//span[contains(normalize-space(),"Close")]]',
        '//button[.//span[contains(normalize-space(),"Accept")]]',
        '//button[contains(@id,"onetrust-accept-btn-handler")]',
    ]
    for xp in close_xpaths:
        try:
            for btn in driver.find_elements(By.XPATH, xp):
                if btn.is_displayed() and btn.is_enabled():
                    force_click(driver, btn)
                    time.sleep(0.3)
        except Exception:
            continue


def _wait_search_results_ready(driver: webdriver.Remote, timeout: int = 45) -> None:
    end_time = time.time() + timeout
    while time.time() < end_time:
        _dismiss_common_popups(driver)
        if driver.find_elements(By.XPATH, '//div[@data-testid="property-card"]'):
            return
        no_results = driver.find_elements(
            By.XPATH,
            '//*[contains(translate(text(),"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"no properties found") or '
            'contains(translate(text(),"ABCDEFGHIJKLMNOPQRSTUVWXYZ","abcdefghijklmnopqrstuvwxyz"),"no results")]',
        )
        if no_results:
            return
        time.sleep(1.0)

    WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((By.XPATH, '//div[@data-testid="property-card"]'))
    )


def _read_description_from_hotel_page(driver: webdriver.Remote) -> str:
    parts: List[str] = []
    for p in driver.find_elements(
        By.XPATH,
        '//p[contains(@class,"b99b6ef58f") and contains(@class,"ffd0a17daf")]',
    ):
        txt = p.text.strip()
        if txt:
            parts.append(txt)

    if not parts:
        for p in driver.find_elements(By.XPATH, '//div[contains(@class,"c3bdfd4ac2")]//p'):
            txt = p.text.strip()
            if txt:
                parts.append(txt)

    if not parts:
        for p in driver.find_elements(By.TAG_NAME, "p"):
            txt = p.text.strip()
            if 60 <= len(txt) <= 1200:
                parts.append(txt)

    clean = _dedupe_keep_order(parts)
    return " | ".join(clean[:6]) if clean else "N/A"


def _room_type_key(room_type: str) -> str:
    normalized = _clean_text(room_type)
    if not normalized or normalized == "N/A":
        return "n/a"
    first_segment = normalized.split("|")[0].strip()
    return re.sub(r"\s+", " ", first_segment).lower()


def _merge_pipe_values(left: str, right: str) -> str:
    left_vals  = [v.strip() for v in str(left).split("|")  if v.strip() and v.strip() != "N/A"]
    right_vals = [v.strip() for v in str(right).split("|") if v.strip() and v.strip() != "N/A"]
    merged = _dedupe_keep_order(left_vals + right_vals)
    return " | ".join(merged) if merged else "N/A"


def _read_coordinates(driver: webdriver.Remote) -> Tuple[Optional[float], Optional[float]]:
    try:
        WebDriverWait(driver, 12).until(
            EC.presence_of_element_located((By.ID, "map_trigger_header_pin"))
        )
        pin    = driver.find_element(By.ID, "map_trigger_header_pin")
        latlng = pin.get_attribute("data-atlas-latlng")
        if not latlng:
            return None, None
        lat, lng = map(float, latlng.split(","))
        return lat, lng
    except Exception:
        return None, None


def _extract_room_rows_from_hotel_page(
    driver: webdriver.Remote,
    hotel_name: str,
    hotel_url: str,
) -> List[Dict[str, object]]:
    rows_out: List[Dict[str, object]] = []
    tbodies = driver.find_elements(By.TAG_NAME, "tbody")
    if not tbodies:
        return rows_out

    for tbody in tbodies:
        rows = tbody.find_elements(By.XPATH, './/tr[@data-block-id]')
        for row in rows:
            try:
                room_type_texts: List[str] = []
                for el in row.find_elements(
                    By.XPATH,
                    './/th[contains(@class,"hprt-table-cell-roomtype") and contains(@class,"droom_seperator") and contains(@class,"canwrap")]//*',
                ):
                    t = _clean_text(el.text)
                    if t:
                        room_type_texts.append(t)
                room_type = " | ".join(_dedupe_keep_order(room_type_texts))
                if not room_type:
                    room_type = _clean_text(
                        row.find_element(
                            By.XPATH,
                            './/th[contains(@class,"hprt-table-cell-roomtype") and contains(@class,"droom_seperator") and contains(@class,"canwrap")]',
                        ).text
                    )
            except Exception:
                room_type = "N/A"

            try:
                guests = _clean_text(
                    row.find_element(
                        By.XPATH,
                        './/td[contains(@class,"hprt-table-cell-occupancy") and contains(@class,"droom_seperator")]',
                    ).text
                )
                guests = guests if guests else "N/A"
            except Exception:
                guests = "N/A"

            try:
                price_texts = [
                    _clean_text(s.text)
                    for s in row.find_elements(
                        By.XPATH, './/span[contains(@class,"prco-valign-middle-helper")]',
                    )
                    if _clean_text(s.text)
                ]
                price_for_5_nights = " | ".join(_dedupe_keep_order(price_texts)) if price_texts else "N/A"
            except Exception:
                price_for_5_nights = "N/A"

            try:
                choices_texts = [
                    _clean_text(el.text)
                    for el in row.find_elements(
                        By.XPATH,
                        './/td[contains(@class,"hprt-table-cell-conditions") and contains(@class,"droom_seperator") and contains(@class,"hprt-block-reposition-tooltip--container")]//*',
                    )
                    if _clean_text(el.text)
                ]
                your_choices = " | ".join(_dedupe_keep_order(choices_texts)) if choices_texts else "N/A"
            except Exception:
                your_choices = "N/A"

            if all(v == "N/A" for v in [room_type, guests, price_for_5_nights, your_choices]):
                continue

            rows_out.append({
                "Hotel Name":         hotel_name,
                "Hotel URL":          hotel_url,
                "Room type":          room_type,
                "Number of guests":   guests,
                "Price for 5 nights": price_for_5_nights,
                "Your choices":       your_choices,
            })

    merged_by_type: Dict[str, Dict[str, object]] = {}
    for r in rows_out:
        key = _room_type_key(str(r.get("Room type", "N/A")))
        if key not in merged_by_type:
            merged_by_type[key] = dict(r)
            continue
        base = merged_by_type[key]
        base["Number of guests"]   = _merge_pipe_values(str(base.get("Number of guests",   "N/A")), str(r.get("Number of guests",   "N/A")))
        base["Price for 5 nights"] = _merge_pipe_values(str(base.get("Price for 5 nights", "N/A")), str(r.get("Price for 5 nights", "N/A")))
        base["Your choices"]       = _merge_pipe_values(str(base.get("Your choices",       "N/A")), str(r.get("Your choices",       "N/A")))

    return list(merged_by_type.values())


def _wait_hotel_page_ready(driver: webdriver.Remote, timeout: int = 20) -> None:
    WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    WebDriverWait(driver, timeout).until(
        lambda d: bool(
            d.find_elements(By.XPATH, '//p[contains(@class,"b99b6ef58f") and contains(@class,"ffd0a17daf")]')
            or d.find_elements(By.ID, "map_trigger_header_pin")
            or d.find_elements(By.XPATH, '//tbody//tr[@data-block-id]')
        )
    )


def _details_are_usable(details: Dict[str, object], room_rows: List[Dict[str, object]]) -> bool:
    if room_rows:
        return True
    if details.get("Description") not in ("N/A", None, ""):
        return True
    if details.get("Latitude") is not None and details.get("Longitude") is not None:
        return True
    return False


def load_cards_until_limit(
    driver: webdriver.Remote,
    target_count: int = 25,
    idle_timeout_s: int = 20,
    step_pause_s: float = 1.2,
) -> None:
    last_count     = 0
    last_growth_ts = time.time()

    while True:
        _dismiss_common_popups(driver)
        cards = driver.find_elements(By.XPATH, '//div[@data-testid="property-card"]')
        count = len(cards)
        print(f"🏨 Loaded cards: {count}")

        if count >= target_count:
            print(f"✅ Reached target of {target_count} hotels.")
            return

        if count > last_count:
            last_count     = count
            last_growth_ts = time.time()
        elif time.time() - last_growth_ts >= idle_timeout_s:
            print("⛔ No additional cards loaded.")
            return

        _scroll_results_container(driver)
        time.sleep(step_pause_s)

        try:
            load_more_btn = driver.find_element(
                By.XPATH,
                '//button[.//span[contains(normalize-space(), "Load more results")]]',
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", load_more_btn)
            force_click(driver, load_more_btn)
            time.sleep(1.8)
        except Exception:
            pass


def _collect_search_cards(driver: webdriver.Remote, limit: int = 25) -> List[Dict[str, object]]:
    hotels     = driver.find_elements(By.XPATH, '//div[@data-testid="property-card"]')
    card_xpath = '(//div[@data-testid="property-card"])[{}]'
    max_to_read = min(limit, len(hotels))
    print(f"🔍 Found {len(hotels)} hotel cards on current page")

    cards: List[Dict[str, object]] = []
    for idx in range(1, max_to_read + 1):
        card_data: Dict[str, object] = {
            "Hotel Name": "N/A", "Price": "N/A",
            "Review Score (/10)": "N/A", "Hotel URL": "N/A",
        }
        for _ in range(2):
            try:
                hotel = driver.find_element(By.XPATH, card_xpath.format(idx))
                try:
                    card_data["Hotel Name"] = hotel.find_element(
                        By.XPATH, './/div[@data-testid="title"]'
                    ).text.strip()
                except Exception:
                    pass
                try:
                    price_elem  = hotel.find_element(
                        By.XPATH, './/span[contains(@class,"b87c397a13") and contains(@class,"f2f358d1de")]',
                    )
                    price_clean = re.sub(r"[^\d]", "", price_elem.text)
                    card_data["Price"] = int(price_clean) if price_clean else "N/A"
                except Exception:
                    pass
                try:
                    review = hotel.find_element(
                        By.XPATH, './/div[contains(@class,"f63b14ab7a") and contains(@class,"dff2e52086")]',
                    ).text
                    card_data["Review Score (/10)"] = float(review.replace(",", "."))
                except Exception:
                    pass
                try:
                    card_data["Hotel URL"] = hotel.find_element(
                        By.XPATH, './/a[@data-testid="title-link"]'
                    ).get_attribute("href")
                except Exception:
                    pass
                break
            except StaleElementReferenceException:
                time.sleep(0.3)
                continue
            except Exception:
                break
        cards.append(card_data)
    return cards


def _fetch_hotel_details(
    hotel: Dict[str, object],
    event_coords: Tuple[float, float],
) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    url        = str(hotel.get("Hotel URL", "N/A"))
    hotel_name = str(hotel.get("Hotel Name", "N/A"))

    if url == "N/A":
        return {"Description": "N/A", "Latitude": None, "Longitude": None, "Distance (km)": "N/A"}, []

    driver = create_driver()   # retry logic is inside create_driver
    try:
        fallback_details = {"Description": "N/A", "Latitude": None, "Longitude": None, "Distance (km)": "N/A"}
        fallback_rows: List[Dict[str, object]] = []

        for attempt in range(2):
            try:
                driver.get(url)
                _wait_hotel_page_ready(driver, timeout=22)
                time.sleep(1.6)

                description = _read_description_from_hotel_page(driver)
                lat, lng    = _read_coordinates(driver)
                distance    = round(geodesic((lat, lng), event_coords).km, 2) if lat is not None and lng is not None else "N/A"
                room_rows   = _extract_room_rows_from_hotel_page(driver, hotel_name, url)
                details     = {"Description": description, "Latitude": lat, "Longitude": lng, "Distance (km)": distance}

                fallback_details = details
                fallback_rows    = room_rows

                if _details_are_usable(details, room_rows):
                    return details, room_rows

                if attempt == 0:
                    driver.refresh()
                    time.sleep(1.2)
            except Exception:
                if attempt == 0:
                    time.sleep(1.2)
                    continue

        return fallback_details, fallback_rows
    except Exception:
        return {"Description": "N/A", "Latitude": None, "Longitude": None, "Distance (km)": "N/A"}, []
    finally:
        # FIX: session may have already timed out on the Grid side while
        # the thread was waiting — swallow the error, data is already saved.
        try:
            driver.quit()
        except Exception:
            pass


def _enrich_by_opening_hotel_pages_multithread(
    cards: List[Dict[str, object]],
    event_coords: Tuple[float, float],
    max_workers: int = 3,   # FIX: reduced from 4 → 3 (safer with SE_NODE_MAX_SESSIONS=5)
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    enriched      = [dict(c) for c in cards]
    all_room_rows: List[Dict[str, object]] = []

    futures = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for idx, hotel in enumerate(enriched):
            futures[executor.submit(_fetch_hotel_details, hotel, event_coords)] = idx

        for future in as_completed(futures):
            idx = futures[future]
            try:
                details, room_rows = future.result()
            except Exception:
                details   = {"Description": "N/A", "Latitude": None, "Longitude": None, "Distance (km)": "N/A"}
                room_rows = []
            enriched[idx].update(details)
            all_room_rows.extend(room_rows)

    return enriched, all_room_rows


# =============================================================================
#   MAIN FLOW
# =============================================================================
def run_scraping(
    destination: str,
    checkin: str,
    checkout: str,
    hotel_limit: int = 25,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    driver       = create_driver()   # retry logic inside
    event_coords = (30.0444, 31.2357)

    try:
        search_url = _build_search_url(destination, checkin, checkout)
        print("🌍 Opening search:", search_url)
        driver.get(search_url)

        _wait_search_results_ready(driver, timeout=55)

        if not driver.find_elements(By.XPATH, '//div[@data-testid="property-card"]'):
            time.sleep(2.0)
            _dismiss_common_popups(driver)
            driver.refresh()
            _wait_search_results_ready(driver, timeout=45)

        load_cards_until_limit(driver, target_count=hotel_limit)
        cards = _collect_search_cards(driver, limit=hotel_limit)

        if not cards:
            raise RuntimeError("No hotel cards were loaded from Booking search page.")

        unique_hotels: List[Dict[str, object]] = []
        seen_urls: Set[str] = set()
        for hotel in cards:
            hotel_url = str(hotel.get("Hotel URL", "N/A"))
            if hotel_url != "N/A":
                if hotel_url in seen_urls:
                    continue
                seen_urls.add(hotel_url)
            unique_hotels.append(hotel)

        unique_hotels  = unique_hotels[:hotel_limit]
        enriched_hotels, room_rows = _enrich_by_opening_hotel_pages_multithread(
            unique_hotels, event_coords=event_coords, max_workers=3
        )

        today           = datetime.now().strftime("%Y-%m-%d")
        hotels_df       = pd.DataFrame(enriched_hotels)
        rooms_df        = pd.DataFrame(room_rows)
        file_name_hotel = f"{destination}_{today}_booking.csv"
        file_name_room  = f"{destination}_{today}_booking_rooms.csv"

        hotels_df.to_csv(f"{OUTPUT_DIR}/{file_name_hotel}", index=False, encoding="utf-8-sig")
        rooms_df.to_csv(f"{OUTPUT_DIR}/{file_name_room}",   index=False, encoding="utf-8-sig")

        print(f"\n💾 Saved {len(hotels_df)} hotels   → {file_name_hotel}")
        print(f"💾 Saved {len(rooms_df)} room rows → {file_name_room}")
        return hotels_df, rooms_df
    finally:
        # FIX: the main search session can expire while threads scrape hotel pages.
        # Grid default inactivity timeout is 300s. Swallow quit() errors — all
        # data is already written to disk before this point.
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    for city in CITIES:
        print(f"\n{'='*50}\n🏙️  Scraping: {city}\n{'='*50}")
        hotels_df, rooms_df = run_scraping(city, CHECKIN, CHECKOUT, hotel_limit=LIMIT)
        print(hotels_df.head())
        print("\n--- Rooms preview ---")
        print(rooms_df.head())
    print("\n✅ Booking scraping complete.")