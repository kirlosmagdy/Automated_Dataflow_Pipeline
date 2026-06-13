"""
transformation_script.py
────────────────────────
Transforms and merges raw scraped CSV files into clean, Supabase-ready files.

FIX: Instead of using TODAY's date to find files (which breaks when scraping
     runs overnight and transform runs the next morning), we now use glob to
     find the most recently created file matching each pattern. This makes the
     script date-independent.

Steps per dataset:
    1. Find the latest matching CSV using glob
    2. Add City column to each file
    3. Merge all city files into one CSV
    4. Add an integer ID column
    5. Move City to the second column
    6. Rename columns to match Supabase table schema

Input  folder: /opt/airflow/scraping_scripts/Data_files/
Output folder: /opt/airflow/database/Data_files/

Output files:
    restaurants.csv
    hotels.csv
    room_hotels.csv
    places.csv  (expected to already exist, just gets columns renamed)
"""

import pandas as pd
from glob import glob
from datetime import datetime
import os

# ── Folder paths ──────────────────────────────────────────────────────────────
SCRAPING_DIR = "/opt/airflow/scraping_scripts/Data_files"
DATABASE_DIR = "/opt/airflow/database/Data_files"


# =============================================================================
#   HELPER: Find the most recently modified file matching a glob pattern
# =============================================================================
def find_latest_file(pattern: str) -> str | None:
    """
    Find the most recently modified file matching a glob pattern.

    Args:
        pattern: glob pattern, e.g. '/opt/airflow/.../alex_*_talabat.csv'

    Returns:
        Full path to the most recent matching file, or None if not found.

    Example:
        find_latest_file('.../alex_*_talabat.csv')
        → '.../alex_2026-05-09_talabat.csv'   ← picks the newest automatically
    """
    matches = glob(pattern)
    if not matches:
        return None
    # Sort by file modification time, return the newest
    return max(matches, key=os.path.getmtime)


# =============================================================================
#   HELPER: Merge city CSVs, add ID, move City to column 2
# =============================================================================
def merge_and_prepare(city_patterns: dict, output_path: str) -> pd.DataFrame:
    """
    For each city, find the latest matching file, add a City column, then
    merge all cities into one DataFrame with an integer ID column.

    Args:
        city_patterns: {city_label: glob_pattern}
                       e.g. {"cairo": ".../cairo_*_talabat.csv"}
        output_path:   where to save the merged intermediate CSV

    Returns:
        The merged and prepared DataFrame.
    """
    dfs = {}

    for city, pattern in city_patterns.items():
        latest = find_latest_file(pattern)

        if latest is None:
            print(f"  ⚠️  No file found for pattern: {pattern} — skipping {city}")
            continue

        try:
            df = pd.read_csv(latest)
            df["City"] = city
            dfs[city] = df
            print(f"  ✔ Loaded [{city}]: {os.path.basename(latest)}")
        except Exception as e:
            print(f"  ⚠️  Could not read {latest}: {e} — skipping")

    if not dfs:
        raise RuntimeError(
            "No files were loaded for any city. "
            "Check that scraping completed and files exist in the scraping directory."
        )

    merged = pd.concat(dfs.values(), ignore_index=True)

    # Add integer ID as first column
    merged.insert(0, "id", range(1, len(merged) + 1))

    # Move City to second column (right after id)
    cols = list(merged.columns)
    cols.insert(1, cols.pop(cols.index("City")))
    merged = merged[cols]

    merged.to_csv(output_path, index=False)
    return merged


# =============================================================================
#   DATASET TRANSFORMS
# =============================================================================
def prepare_restaurants() -> None:
    print("\n📂 Preparing: restaurants")

    # Use glob patterns — picks the latest file regardless of scrape date
    city_patterns = {
        "alexandria": f"{SCRAPING_DIR}/alex_*_talabat.csv",
        "cairo":      f"{SCRAPING_DIR}/cairo_*_talabat.csv",
    }
    output = f"{DATABASE_DIR}/restaurants.csv"
    df = merge_and_prepare(city_patterns, output)

    df = df.rename(columns={
        "id":          "restaurant_id",
        "City":        "city",
        "Restaurant":  "restaurant_name",
        "Location":    "location",
        "Cuisines":    "cuisines",
        "URL":         "url",
        "Total_Items": "total_items",
        "Prices_List": "prices_list",
        "Min_Price":   "min_price",
        "Max_Price":   "max_price",
        "Avg_Price":   "avg_price",
    })
    df.to_csv(output, index=False)
    print(f"  ✔ Saved {len(df)} rows → restaurants.csv")


def prepare_hotels() -> None:
    print("\n📂 Preparing: hotels")

    city_patterns = {
        "alexandria": f"{SCRAPING_DIR}/Alexandria_*_booking.csv",
        "cairo":      f"{SCRAPING_DIR}/Cairo_*_booking.csv",
        "luxor":      f"{SCRAPING_DIR}/Luxor_*_booking.csv",
        "sharm":      f"{SCRAPING_DIR}/Sharm El Sheikh_*_booking.csv",
    }
    output = f"{DATABASE_DIR}/hotels.csv"
    df = merge_and_prepare(city_patterns, output)

    df = df.rename(columns={
        "id":                  "hotel_id",
        "City":                "city",
        "Hotel Name":          "hotel_name",
        "Price":               "price",
        "Review Score (/10)":  "review_score",
        "Hotel URL":           "hotel_url",
        "Description":         "description",
        "Latitude":            "latitude",
        "Longitude":           "longitude",
        "Distance (km)":       "distance_km",
    })
    df.to_csv(output, index=False)
    print(f"  ✔ Saved {len(df)} rows → hotels.csv")


def prepare_room_hotels() -> None:
    print("\n📂 Preparing: room_hotels")

    city_patterns = {
        "alexandria": f"{SCRAPING_DIR}/Alexandria_*_booking_rooms.csv",
        "cairo":      f"{SCRAPING_DIR}/Cairo_*_booking_rooms.csv",
        "luxor":      f"{SCRAPING_DIR}/Luxor_*_booking_rooms.csv",
        "sharm":      f"{SCRAPING_DIR}/Sharm El Sheikh_*_booking_rooms.csv",
    }
    output = f"{DATABASE_DIR}/room_hotels.csv"
    df = merge_and_prepare(city_patterns, output)

    df = df.rename(columns={
        "id":                  "room_id",
        "City":                "city",
        "Hotel Name":          "hotel_name",
        "Hotel URL":           "hotel_url",
        "Room type":           "room_name",
        "Number of guests":    "number_of_guests",
        "Price for 5 nights":  "price_5_nights",
        "Your choices":        "your_choices",
    })
    df.to_csv(output, index=False)
    print(f"  ✔ Saved {len(df)} rows → room_hotels.csv")


def prepare_places() -> None:
    """
    Places data is not scraped weekly — just rename columns to match Supabase.
    Expects places.csv to already exist in the database folder.
    """
    print("\n📂 Preparing: places (column rename only)")
    path = f"{DATABASE_DIR}/places.csv"
    try:
        df = pd.read_csv(path)
        df = df.rename(columns={
            "id":               "place_id",
            "City":             "city",
            "Title":            "title",
            "Rating":           "rating",
            "Reviews":          "reviews",
            "Description":      "description",
            "Tips":             "tips",
            "Address":          "address",
            "Timings":          "timings",
            "Ticket Price":     "ticket_price",
            "Avg_Ticket_Price": "avg_ticket_price",
        })
        df.to_csv(path, index=False)
        print(f"  ✔ Saved {len(df)} rows → places.csv")
    except FileNotFoundError:
        print("  ⚠️  places.csv not found — skipping.")


# =============================================================================
#   ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    print("🚀 Starting transformation pipeline...")
    print(f"   Scraping dir : {SCRAPING_DIR}")
    print(f"   Database dir : {DATABASE_DIR}")

    prepare_restaurants()
    prepare_hotels()
    prepare_room_hotels()
    prepare_places()

    print("\n✅ All transformations complete.")