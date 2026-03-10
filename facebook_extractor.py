import sys
import requests
from bs4 import BeautifulSoup
import time
import re
import csv
import io
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Force UTF-8 output on Windows to avoid encoding crashes
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
SHEET_ID = "1epekP6pptvzWpkEI-5-nsXv9iR8M4zX8QsOb5SfrZag"
GID = "1290806086"   # Sheet9
SHEET_CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
CREDS_FILE = "ticket-automation-478816-065d3bcabe7e.json"

OUTPUT_FILE = "facebook_scrape_results_sheet9.csv"
DELAY_BETWEEN_REQUESTS = 0.5   # seconds (faster but still polite)
MAX_RETRIES = 2
TIMEOUT = 10
SHEET_BATCH_SIZE = 10          # Write to Google Sheet every N scraped rows

# Column header names (case-insensitive matching)
COL_BRAND = "brand name"
COL_SHOPIFY = "shopify store"
COL_WEBSITE = "website"
COL_FB_PROFILE = "facebook profile"

# Strings that indicate no store exists
NO_STORE_MARKERS = ["no shopify store found", "shopify site missing", "n/a", ""]

# Invalid FB URL paths to filter out
INVALID_FB_PATHS = ["/sharer", "/share.php", "/dialog/", "plugins/", "/tr?", "pixel", "/ads/", "facebook.com/login", "facebook.com/help"]

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def normalize_url(url):
    """Ensure URL has a scheme and strip whitespace."""
    url = url.strip()
    if not url:
        return None
    # Already has scheme
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return "http://" + url


def is_valid_fb_url(url):
    """Return True if the URL looks like a real FB profile/page, not a tracker/sharer."""
    url_lower = url.lower()
    # Must contain facebook.com
    if "facebook.com" not in url_lower:
        return False
    # Filter noise paths
    if any(bad in url_lower for bad in INVALID_FB_PATHS):
        return False
    # Filter very short paths (e.g. facebook.com/ or facebook.com/tr)
    path_match = re.search(r'facebook\.com/([^/?#\s"\']+)', url_lower)
    if not path_match:
        return False
    path = path_match.group(1)
    if len(path) < 3:   # too short to be a real page
        return False
    return True


def clean_fb_url(url):
    """Strip query params and trailing noise from a Facebook URL."""
    # Remove query string and fragments
    url = url.split('?')[0].split('#')[0]
    # Strip trailing slashes or noise chars
    url = url.rstrip('/ \\"\';')
    return url


def extract_facebook_url(store_url):
    """
    Loads the store page and searches the ENTIRE HTML source for Facebook mentions.
    Returns the best Facebook URL found, or a status string.
    """
    normalized = normalize_url(store_url)
    if not normalized:
        return "No URL"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(normalized, headers=headers, timeout=TIMEOUT, allow_redirects=True)
            response.raise_for_status()
            break
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response else 0
            if code == 429 or code == 503:
                # Rate-limited; wait longer before retry
                wait = attempt * 5
                time.sleep(wait)
                if attempt == MAX_RETRIES:
                    return f"Error {code}: Rate limited"
            else:
                return f"Error {code}: {e}"
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES:
                return f"Error: {e}"
            time.sleep(attempt * 2)

    html = response.text

    # ── Strategy 1: Regex across full HTML ──────────────────────────────────
    # Handles FB URLs in <script>, JSON-LD, og:tags, data attributes, everything
    fb_regex = re.compile(
        r'https?://(?:www\.)?(?:m\.)?facebook\.com/[^\s"\'<>\\]+',
        re.IGNORECASE
    )
    raw_matches = fb_regex.findall(html)

    valid_matches = []
    for match in raw_matches:
        cleaned = clean_fb_url(match)
        if is_valid_fb_url(cleaned) and cleaned not in valid_matches:
            valid_matches.append(cleaned)

    if valid_matches:
        return valid_matches[0]

    # ── Strategy 2: BeautifulSoup for relative / encoded hrefs ──────────────
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if "facebook" in href.lower():
            if href.startswith("/"):
                # Relative link — unlikely to be the right FB but worth checking
                continue
            cleaned = clean_fb_url(href)
            if is_valid_fb_url(cleaned):
                return cleaned

    return "No Facebook link found"


# ─────────────────────────────────────────────
# GOOGLE SHEETS FETCH
# ─────────────────────────────────────────────

def fetch_sheet_data():
    """Download the Google Sheet as CSV and return list of dicts."""
    print("[INFO] Fetching data from Google Sheets...")
    resp = requests.get(SHEET_CSV_URL, timeout=30)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text))
    rows = list(reader)
    print(f"   [OK] {len(rows)} rows loaded.\n")
    return reader.fieldnames, rows


# ─────────────────────────────────────────────
# GOOGLE SHEETS AUTH (for writing)
# ─────────────────────────────────────────────

def get_gsheet():
    """Authenticate and return the gspread worksheet object for writing."""
    if not os.path.exists(CREDS_FILE):
        print(f"[WARN] Credentials file '{CREDS_FILE}' not found. Will write to CSV only.")
        return None
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).sheet1
        print("[OK] Authenticated with Google Sheets for direct writing.")
        return sheet
    except Exception as e:
        print(f"[WARN] Could not connect to Google Sheets for writing: {e}")
        print("       Results will be saved to CSV only.")
        return None


def flush_sheet_updates(sheet, updates):
    """Send a batch of cell updates to Google Sheets."""
    if not sheet or not updates:
        return
    try:
        sheet.batch_update(updates)
        print(f"   [SHEET] Wrote {len(updates)} updates to Google Sheets.")
    except Exception as e:
        print(f"   [SHEET-ERR] Failed to write batch: {e}")


# ─────────────────────────────────────────────
# COLUMN DETECTION
# ─────────────────────────────────────────────

def find_col(fieldnames, target):
    """Case-insensitive column name lookup. Returns the exact header string."""
    for name in fieldnames:
        if name.strip().lower() == target.lower():
            return name
    return None


# ─────────────────────────────────────────────
# AUTO-RESUME: load already-processed stores
# ─────────────────────────────────────────────

def load_processed(output_file, shopify_col):
    """Return a set of URLs (Shopify Store or Website) already written to the output CSV."""
    processed = set()
    if not os.path.exists(output_file):
        return processed
    with open(output_file, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Add Shopify Store value if present
            shopify_val = row.get(shopify_col, "").strip()
            if shopify_val:
                processed.add(shopify_val)
            # Add Website value if present (case‑insensitive header handling)
            for key in row.keys():
                if key.strip().lower() == "website":
                    website_val = row.get(key, "").strip()
                    if website_val:
                        processed.add(website_val)
                    break
    return processed


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    fieldnames, rows = fetch_sheet_data()

    # Detect columns dynamically
    col_brand    = find_col(fieldnames, COL_BRAND)
    col_shopify  = find_col(fieldnames, COL_SHOPIFY)
    col_website  = find_col(fieldnames, COL_WEBSITE)
    col_fb       = find_col(fieldnames, COL_FB_PROFILE)

    if not col_fb:
        print("[ERR] Could not find 'Facebook Profile' column in the sheet.")
        print(f"   Available columns: {fieldnames}")
        return

    print(f"[INFO] Columns detected:")
    print(f"   Brand:    '{col_brand}'")
    print(f"   Shopify:  '{col_shopify}'")
    print(f"   Website:  '{col_website}'")
    print(f"   Facebook: '{col_fb}'\n")

    # Determine the column letter/index for the FB column in Google Sheets
    # fieldnames order matches the sheet column order
    fb_col_index = list(fieldnames).index(col_fb) + 1  # 1-based for gspread

    # Authenticate with Google Sheets for direct writing
    sheet = get_gsheet()

    # Auto-resume: find what's already done
    processed_stores = load_processed(OUTPUT_FILE, col_shopify)
    if processed_stores:
        print(f"[RESUME] {len(processed_stores)} rows already processed, skipping them.\n")

    # Prepare output CSV file (backup + crash recovery)
    file_exists = os.path.exists(OUTPUT_FILE)
    out_f = open(OUTPUT_FILE, "a", encoding="utf-8", newline="")
    writer = csv.DictWriter(out_f, fieldnames=fieldnames)
    if not file_exists:
        writer.writeheader()

    total = len(rows)
    skipped = 0
    scraped = 0
    found = 0
    sheet_updates = []  # Queued updates for Google Sheets

    try:
        for i, row in enumerate(rows, 1):
            brand       = row.get(col_brand, "").strip() if col_brand else ""
            shopify_url = row.get(col_shopify, "").strip() if col_shopify else ""
            website_url = row.get(col_website, "").strip() if col_website else ""
            
            fb_existing = row.get(col_fb, "").strip()
            sheet_row   = i + 1   # +1 for header row (1-based in Sheets)

            # 1. Skip if Facebook Profile cell is NOT empty
            if fb_existing:
                skipped += 1
                continue

            # 2. Skip if Website is "NO URL FOUND"
            if website_url.upper() == "NO URL FOUND":
                skipped += 1
                continue

            # 3. Use Website as the source
            source_url = website_url
            if not source_url:
                source_url = shopify_url

            # 4. Skip if no source URL at all
            if not source_url:
                skipped += 1
                continue

            # 5. Skip if already in local CSV output
            if source_url in processed_stores:
                skipped += 1
                continue

            # Scrape
            scraped += 1
            label = f"[{i}/{total}] {brand[:25]:<25}"
            print(f">> {label} | {source_url[:45]:<45}", end=" ... ", flush=True)

            fb_url = extract_facebook_url(source_url)
            row[col_fb] = fb_url

            if "facebook.com" in fb_url.lower():
                found += 1
                print(f"[FOUND] {fb_url}")
            else:
                print(f"[none]  {fb_url}")

            # Write to local CSV (crash recovery)
            writer.writerow(row)
            out_f.flush()

            # Queue update for Google Sheets
            cell_ref = gspread.utils.rowcol_to_a1(sheet_row, fb_col_index)
            sheet_updates.append({'range': cell_ref, 'values': [[fb_url]]})

            # Flush batch to Google Sheets periodically
            if len(sheet_updates) >= SHEET_BATCH_SIZE:
                flush_sheet_updates(sheet, sheet_updates)
                sheet_updates = []

            time.sleep(DELAY_BETWEEN_REQUESTS)

    except KeyboardInterrupt:
        print("\n[STOP] Interrupted by user. Saving progress...")
    finally:
        # Flush any remaining sheet updates
        if sheet_updates:
            flush_sheet_updates(sheet, sheet_updates)
        out_f.close()

    print(f"\n" + "-"*60)
    print(f"[DONE]  Results saved to: {OUTPUT_FILE} + Google Sheets")
    print(f"   Total rows      : {total}")
    print(f"   Scraped         : {scraped}")
    print(f"   Facebook found  : {found}")
    print(f"   Skipped/existing: {skipped}")
    print("-"*60)


if __name__ == "__main__":
    main()
