import sys
import requests
import csv
import io
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Force UTF-8 output on Windows
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# CONFIG
SHEET_ID = "1epekP6pptvzWpkEI-5-nsXv9iR8M4zX8QsOb5SfrZag"
GID1 = "0"              # Sheet1 (Source)
GID9 = "1290806086"     # Sheet9 (Target)
CREDS_FILE = "ticket-automation-478816-065d3bcabe7e.json"

CSV_URL1 = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID1}"
CSV_URL9 = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID9}"

def fetch_csv_data(url):
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text))
    return list(reader)

def main():
    print("[INFO] Fetching data from Sheet1 and Sheet9...")
    data1 = fetch_csv_data(CSV_URL1)
    data9 = fetch_csv_data(CSV_URL9)
    print(f"   [OK] Loaded {len(data1)} rows from Sheet1 and {len(data9)} rows from Sheet9.")

    # Build map: Brand Name -> Facebook Profile (from Sheet1)
    brand_map = {}
    for row in data1:
        brand = row.get("Brand Name", "").strip().lower()
        fb = row.get("Facebook Profile", "").strip()
        if brand and fb:
            brand_map[brand] = fb

    print(f"   [INFO] Found {len(brand_map)} mappings in Sheet1.")

    # Authenticate with Google Sheets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
    client = gspread.authorize(creds)
    sheet9 = client.open_by_key(SHEET_ID).worksheet("Sheet9")
    print("[OK] Authenticated with Google Sheets (Sheet9).")

    # Prepare updates: Find rows in Sheet9 where we have a map
    updates = []
    # Sheet9 headers: ['Brand Name', 'Website', 'Shopify Store', 'Facebook Profile', 'Facebook followers']
    # FB Profile is 4th column (index 4)
    fb_col_index = 4

    matched_count = 0
    for i, row in enumerate(data9, 2): # Start from row 2 (1-based + header)
        brand = row.get("Brand Name", "").strip().lower()
        if brand in brand_map:
            fb_val = brand_map[brand]
            updates.append({
                'range': gspread.utils.rowcol_to_a1(i, fb_col_index),
                'values': [[fb_val]]
            })
            matched_count += 1

    if not updates:
        print("[INFO] No matches found to migrate.")
        return

    print(f"[INFO] Migrating {matched_count} records to Sheet9...")
    
    # Batch update in groups of 100 to stay within limits
    batch_size = 100
    for j in range(0, len(updates), batch_size):
        batch = updates[j:j+batch_size]
        sheet9.batch_update(batch)
        print(f"   [SHEET] Wrote batch {j//batch_size + 1} ({len(batch)} rows).")

    print(f"\n[DONE] Successfully migrated {matched_count} Facebook entries to Sheet9.")

if __name__ == "__main__":
    main()
