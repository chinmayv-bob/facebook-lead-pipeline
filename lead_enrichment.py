"""
Lead Enrichment Pipeline — Top 50 Hot Leads for BusinessOnBot
=============================================================
Reads 329 brands with Facebook URLs from Google Sheet,
enriches them with Apify data, scores them, and writes results back.
"""
import sys
import requests
import csv
import io
import os
import time
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
PROFILE_ACTOR = "4Hv5RhChiaDk6iwad"
POSTS_ACTOR   = "apify~facebook-posts-scraper"
POSTS_LIMIT   = 10  # last N posts per brand

SHEET_ID   = "1epekP6pptvzWpkEI-5-nsXv9iR8M4zX8QsOb5SfrZag"
GID        = "1290806086"   # Sheet9
CREDS_FILE = "ticket-automation-478816-065d3bcabe7e.json"
SHEET_CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
SHEET_TAB  = "All-Leads"

OUTPUT_FILE     = "lead_enrichment_results.csv"
RAW_OUTPUT_FILE = "lead_enrichment_raw.json"   # full unprocessed API responses
DELAY_BETWEEN_BRANDS = 1  # seconds between brands (polite spacing)
BATCH_LIMIT = 100         # max brands to process per run (auto-resume picks up where left off)

# Column names (case-insensitive matching)
COL_BRAND      = "brand name"
COL_SHOPIFY    = "shopify store"
COL_WEBSITE    = "website"
COL_FB_PROFILE = "facebook profile"

# Sync endpoint timeout (Apify allows up to 300s)
SYNC_TIMEOUT = 120  # seconds


# ─────────────────────────────────────────────
# APIFY API — SEPARATE FUNCTIONS
# ─────────────────────────────────────────────

class ApifyCreditsError(Exception):
    """Raised when Apify returns 402 (credits exhausted) or 429 (rate limit)."""
    pass

def fetch_profile_details(fb_url):
    """
    Call Apify Profile Details actor synchronously.
    Returns dict with profile data, or None on failure.
    """
    url = f"https://api.apify.com/v2/acts/{PROFILE_ACTOR}/run-sync-get-dataset-items"
    headers = {
        "Authorization": f"Bearer {APIFY_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "startUrls": [{"url": fb_url}]
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=SYNC_TIMEOUT)
        if resp.status_code >= 400:
            raise ApifyCreditsError(f"Apify API Error {resp.status_code} — credits likely exhausted\\n{resp.text[:200]}")
        resp.raise_for_status()
        items = resp.json()
        if items and len(items) > 0:
            return items[0]
        return None
    except ApifyCreditsError:
        raise  # re-raise so main loop can abort
    except requests.exceptions.Timeout:
        print(f"      [TIMEOUT] Profile details timed out for {fb_url}")
        return None
    except Exception as e:
        print(f"      [ERR] Profile details failed: {e}")
        return None


def fetch_recent_posts(fb_url):
    """
    Call Apify Posts Scraper actor synchronously.
    Returns list of post dicts, or empty list on failure.
    """
    url = f"https://api.apify.com/v2/acts/{POSTS_ACTOR}/run-sync-get-dataset-items"
    headers = {
        "Authorization": f"Bearer {APIFY_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "startUrls": [{"url": fb_url}],
        "resultsLimit": POSTS_LIMIT
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=SYNC_TIMEOUT)
        if resp.status_code >= 400:
            raise ApifyCreditsError(f"Apify API Error {resp.status_code} — credits likely exhausted\\n{resp.text[:200]}")
        resp.raise_for_status()
        items = resp.json()
        return items if items else []
    except ApifyCreditsError:
        raise  # re-raise so main loop can abort
    except requests.exceptions.Timeout:
        print(f"      [TIMEOUT] Posts scraper timed out for {fb_url}")
        return []
    except Exception as e:
        print(f"      [ERR] Posts scraper failed: {e}")
        return []


# ─────────────────────────────────────────────
# DATA EXTRACTION HELPERS
# ─────────────────────────────────────────────

def extract_profile_metrics(profile):
    """Extract key metrics from Apify profile response."""
    if not profile:
        return {
            "followers": 0,
            "likes": 0,
            "category": "",
            "email": "",
            "phone": "",
            "website": "",
            "ad_status": "",
            "creation_date": "",
            "intro": "",
        }
    return {
        "followers": profile.get("followers", 0) or 0,
        "likes": profile.get("likes", 0) or 0,
        "category": profile.get("category", "") or "",
        "email": profile.get("email", "") or "",
        "phone": profile.get("phone", "") or "",
        "website": profile.get("website", "") or "",
        "ad_status": profile.get("ad_status", "") or "",
        "creation_date": profile.get("creation_date", "") or "",
        "intro": (profile.get("intro", "") or "")[:150],
    }


def extract_post_metrics(posts):
    """Calculate engagement metrics from posts list."""
    if not posts:
        return {
            "total_posts": 0,
            "avg_likes": 0,
            "avg_shares": 0,
            "avg_views": 0,
            "avg_reactions": 0,
            "posts_last_30d": 0,
            "last_post_date": "",
        }

    total = len(posts)
    total_likes = sum(p.get("likes", 0) or 0 for p in posts)
    total_shares = sum(p.get("shares", 0) or 0 for p in posts)
    total_views = sum(p.get("viewsCount", 0) or 0 for p in posts)
    total_reactions = sum(p.get("topReactionsCount", 0) or 0 for p in posts)

    # Count posts in last 30 days + find most recent post date
    now = datetime.now(timezone.utc)
    posts_last_30d = 0
    latest_date = ""
    for p in posts:
        ts = p.get("timestamp")
        if ts:
            try:
                post_time = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                date_str = post_time.strftime("%Y-%m-%d")
                if not latest_date or date_str > latest_date:
                    latest_date = date_str
                if (now - post_time).days <= 30:
                    posts_last_30d += 1
            except (ValueError, OSError):
                pass
        elif not latest_date and p.get("time"):
            # Fallback to ISO time string
            latest_date = str(p["time"])[:10]

    return {
        "total_posts": total,
        "avg_likes": round(total_likes / total, 1) if total else 0,
        "avg_shares": round(total_shares / total, 1) if total else 0,
        "avg_views": round(total_views / total, 1) if total else 0,
        "avg_reactions": round(total_reactions / total, 1) if total else 0,
        "posts_last_30d": posts_last_30d,
        "last_post_date": latest_date,
    }


# ─────────────────────────────────────────────
# LEAD SCORING
# ─────────────────────────────────────────────

def calculate_lead_score(profile_metrics, post_metrics):
    """
    Score a lead from 0-100 based on:
      - Followers       (25%)
      - Running ads     (20%)
      - Post frequency  (20%)
      - Avg engagement  (20%)
      - Contact info    (15%)
    """
    score = 0.0

    # 1. Followers (25 pts) — log scale to avoid mega-brands dominating
    followers = profile_metrics["followers"]
    if followers >= 100000:
        score += 25
    elif followers >= 50000:
        score += 22
    elif followers >= 10000:
        score += 18
    elif followers >= 5000:
        score += 14
    elif followers >= 1000:
        score += 10
    elif followers >= 500:
        score += 6
    elif followers > 0:
        score += 3

    # 2. Running ads (20 pts) — binary
    if "running ads" in profile_metrics["ad_status"].lower():
        score += 20

    # 3. Post frequency — posts in last 30 days (20 pts)
    p30 = post_metrics["posts_last_30d"]
    if p30 >= 8:
        score += 20
    elif p30 >= 5:
        score += 16
    elif p30 >= 3:
        score += 12
    elif p30 >= 1:
        score += 8
    elif post_metrics["total_posts"] > 0:
        score += 3

    # 4. Avg engagement per post (20 pts)
    avg_eng = post_metrics["avg_likes"] + post_metrics["avg_shares"] + post_metrics["avg_reactions"]
    if avg_eng >= 100:
        score += 20
    elif avg_eng >= 50:
        score += 16
    elif avg_eng >= 20:
        score += 12
    elif avg_eng >= 5:
        score += 8
    elif avg_eng > 0:
        score += 4

    # 5. Contact info available (15 pts)
    if profile_metrics["email"]:
        score += 8
    if profile_metrics["phone"]:
        score += 7

    return round(score)


# ─────────────────────────────────────────────
# SHEET HELPERS
# ─────────────────────────────────────────────

def fetch_sheet_data():
    """Download sheet as CSV, return fieldnames and rows."""
    print("[INFO] Fetching data from Google Sheets...")
    resp = requests.get(SHEET_CSV_URL, timeout=30)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text))
    rows = list(reader)
    print(f"   [OK] {len(rows)} rows loaded.\n")
    return reader.fieldnames, rows


def find_col(fieldnames, target):
    for name in fieldnames:
        if name.strip().lower() == target.lower():
            return name
    return None


def get_gsheet():
    """Authenticate for writing."""
    if not os.path.exists(CREDS_FILE):
        print(f"[WARN] Credentials file not found. CSV only.")
        return None
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_TAB)
        print(f"[OK] Authenticated with Google Sheets (tab: {SHEET_TAB}).\n")
        return sheet
    except Exception as e:
        print(f"[WARN] Sheet auth failed: {e}")
        return None


def load_processed(output_file):
    """Return set of FB URLs already processed."""
    processed = set()
    if not os.path.exists(output_file):
        return processed
    with open(output_file, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            val = row.get("facebook_url", "").strip()
            if val:
                processed.add(val)
    return processed


def load_raw_json(raw_file):
    """Load existing raw JSON file, or return empty dict."""
    if os.path.exists(raw_file):
        with open(raw_file, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}


def save_raw_json(raw_file, raw_data):
    """Persist the raw_data dict to disk (overwrites with full updated dict)."""
    with open(raw_file, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, ensure_ascii=False, indent=2)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

ENRICHED_FIELDS = [
    "brand_name", "shopify_store", "facebook_url",
    "followers", "likes", "category", "email", "phone",
    "website", "ad_status", "creation_date", "intro",
    "total_posts", "avg_likes", "avg_shares", "avg_views",
    "avg_reactions", "posts_last_30d", "last_post_date", "lead_score",
]


def main():
    fieldnames, rows = fetch_sheet_data()

    col_brand   = find_col(fieldnames, COL_BRAND)
    col_shopify = find_col(fieldnames, COL_SHOPIFY)
    col_website = find_col(fieldnames, COL_WEBSITE)
    col_fb      = find_col(fieldnames, COL_FB_PROFILE)

    if not col_fb:
        print("[ERR] Facebook Profile column not found.")
        return

    # Filter to only rows WITH a valid, specific Facebook URL
    # Skip generic profile.php links — they return no useful data from Apify
    fb_rows = []
    skipped_generic = 0
    for row in rows:
        fb_url = row.get(col_fb, "").strip()
        if fb_url and "facebook.com" in fb_url.lower():
            if "profile.php" in fb_url.lower():
                skipped_generic += 1
            else:
                fb_rows.append(row)

    print(f"[INFO] {len(fb_rows)} brands with specific Facebook URLs to enrich.")
    print(f"[INFO] Skipped {skipped_generic} generic profile.php links.\n")

    # Authenticate for Sheet writing
    sheet = get_gsheet()

    # Auto-resume
    processed = load_processed(OUTPUT_FILE)
    if processed:
        print(f"[RESUME] {len(processed)} brands already enriched, skipping them.\n")

    # Load existing raw JSON (auto-resume safe)
    raw_data = load_raw_json(RAW_OUTPUT_FILE)

    # Prepare output CSV
    file_exists = os.path.exists(OUTPUT_FILE)
    out_f = open(OUTPUT_FILE, "a", encoding="utf-8", newline="")
    writer = csv.DictWriter(out_f, fieldnames=ENRICHED_FIELDS)
    if not file_exists:
        writer.writeheader()

    total = len(fb_rows)
    enriched_count = 0
    sheet_updates = []

    # Find column indices for new data in Google Sheet
    # We'll add new columns after the existing ones
    # First, ensure headers exist on the sheet
    new_cols = ["FB Followers", "FB Likes", "FB Category", "FB Email",
                "FB Phone", "FB Ad Status", "FB Posts/30d",
                "Avg Engagement", "Last Post Date", "Lead Score"]

    existing_headers = []
    new_col_indices = {}
    if sheet:
        try:
            existing_headers = sheet.row_values(1)
        except Exception:
            existing_headers = []

        # If sheet is completely empty, write all original headers first
        if not existing_headers:
            base_headers = list(fieldnames)  # from CSV export
            sheet.update('A1', [base_headers])
            existing_headers = base_headers
            print(f"[OK] Wrote base headers to empty sheet: {base_headers}")

        # Add missing enrichment headers
        next_col = len(existing_headers) + 1
        for col_name in new_cols:
            if col_name in existing_headers:
                new_col_indices[col_name] = existing_headers.index(col_name) + 1
            else:
                new_col_indices[col_name] = next_col
                sheet.update_cell(1, next_col, col_name)
                existing_headers.append(col_name)
                next_col += 1
        print(f"[OK] Sheet headers ready. New columns start at col {list(new_col_indices.values())[0]}.\n")

    # ── Thread-safety primitives ──────────────────────────────────────────
    import threading
    write_lock      = threading.Lock()   # guards CSV + JSON writes
    count_lock      = threading.Lock()   # guards enriched_count
    abort_event     = threading.Event()  # set when credits are exhausted
    MAX_WORKERS     = 1                  # sequential – Apify plan limits to 1 concurrent run
    enriched_count  = 0                  # reset (was declared above, re-declare thread-scoped)
    sheet_updates   = []

    def enrich_brand(row, row_index):
        """Worker: enrich one brand and return the result dict or None."""
        nonlocal enriched_count, raw_data

        if abort_event.is_set():
            return None

        brand  = row.get(col_brand, "").strip() if col_brand else ""
        shopify = ""
        if col_shopify:
            shopify = row.get(col_shopify, "").strip()
        elif col_website:
            shopify = row.get(col_website, "").strip()
        fb_url = row.get(col_fb, "").strip()

        print(f">> [{row_index}/{total}] {brand[:30]:<30} | {fb_url[:50]}")

        # ── Call both Apify APIs ──
        print(f"   [{brand[:15]}] Fetching profile...", flush=True)
        profile  = fetch_profile_details(fb_url)   # may raise ApifyCreditsError
        profile_m = extract_profile_metrics(profile)

        if abort_event.is_set():
            return None

        print(f"   [{brand[:15]}] Fetching posts...", flush=True)
        posts  = fetch_recent_posts(fb_url)         # may raise ApifyCreditsError
        post_m = extract_post_metrics(posts)

        score = calculate_lead_score(profile_m, post_m)
        print(f"   [{brand[:15]}] Score: {score}/100 | Followers: {profile_m['followers']}")

        enriched_row = {
            "brand_name": brand, "shopify_store": shopify,
            "facebook_url": fb_url, **profile_m, **post_m,
            "lead_score": score,
        }
        raw_entry = {
            "brand_name": brand, "facebook_url": fb_url,
            "shopify_store": shopify, "profile_data": profile,
            "posts_data": posts, "lead_score": score,
        }

        # ── Thread-safe writes ──
        with write_lock:
            nonlocal enriched_count
            writer.writerow(enriched_row)
            out_f.flush()
            raw_data[brand] = raw_entry
            save_raw_json(RAW_OUTPUT_FILE, raw_data)
            enriched_count += 1
            print(f"   [{brand[:15]}] Saved ({enriched_count} done).\n")

        # Return sheet update payload
        if sheet and new_col_indices:
            sheet_row_num = rows.index(row) + 2
            avg_eng = post_m["avg_likes"] + post_m["avg_shares"] + post_m["avg_reactions"]
            return [
                {"range": gspread.utils.rowcol_to_a1(sheet_row_num, new_col_indices["FB Followers"]),   "values": [[profile_m["followers"]]]},
                {"range": gspread.utils.rowcol_to_a1(sheet_row_num, new_col_indices["FB Likes"]),       "values": [[profile_m["likes"]]]},
                {"range": gspread.utils.rowcol_to_a1(sheet_row_num, new_col_indices["FB Category"]),    "values": [[profile_m["category"]]]},
                {"range": gspread.utils.rowcol_to_a1(sheet_row_num, new_col_indices["FB Email"]),       "values": [[profile_m["email"]]]},
                {"range": gspread.utils.rowcol_to_a1(sheet_row_num, new_col_indices["FB Phone"]),       "values": [[profile_m["phone"]]]},
                {"range": gspread.utils.rowcol_to_a1(sheet_row_num, new_col_indices["FB Ad Status"]),   "values": [[profile_m["ad_status"]]]},
                {"range": gspread.utils.rowcol_to_a1(sheet_row_num, new_col_indices["FB Posts/30d"]),   "values": [[post_m["posts_last_30d"]]]},
                {"range": gspread.utils.rowcol_to_a1(sheet_row_num, new_col_indices["Avg Engagement"]), "values": [[avg_eng]]},
                {"range": gspread.utils.rowcol_to_a1(sheet_row_num, new_col_indices["Last Post Date"]), "values": [[post_m["last_post_date"]]]},
                {"range": gspread.utils.rowcol_to_a1(sheet_row_num, new_col_indices["Lead Score"]),     "values": [[score]]},
            ]
        return []

    # ── Build the work queue (skip already processed, respect BATCH_LIMIT) ──
    work_queue = []
    for i, row in enumerate(fb_rows, 1):
        fb_url = row.get(col_fb, "").strip()
        if fb_url in processed:
            continue
        work_queue.append((row, i))
        if len(work_queue) >= BATCH_LIMIT:
            break

    print(f"[INFO] {len(work_queue)} brands queued for this batch (limit: {BATCH_LIMIT}).\n")

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(enrich_brand, row, idx): (row, idx) for row, idx in work_queue}

            for future in as_completed(futures):
                if abort_event.is_set():
                    break
                try:
                    updates = future.result()
                    if updates:
                        sheet_updates.extend(updates)
                    # Flush sheet every 5 brands from main thread (thread-safe)
                    if enriched_count > 0 and enriched_count % 5 == 0 and sheet_updates:
                        try:
                            sheet.batch_update(sheet_updates)
                            print(f"   [SHEET] Flushed {len(sheet_updates)} updates.")
                            sheet_updates = []
                        except Exception as e:
                            print(f"   [SHEET-ERR] {e}")
                except ApifyCreditsError as e:
                    print(f"\n[ABORT] {e}")
                    print("[ABORT] Setting abort flag — no more brands will be processed.")
                    abort_event.set()

        if abort_event.is_set():
            print("[ABORT] All threads stopped. All completed data has been saved.")

        if len(work_queue) >= BATCH_LIMIT:
            print(f"\n[BATCH] Reached batch limit of {BATCH_LIMIT}. Run again to continue.")
    finally:
        # Flush remaining sheet updates
        if sheet and sheet_updates:
            try:
                sheet.batch_update(sheet_updates)
                print(f"   [SHEET] Wrote final {len(sheet_updates)} updates.")
            except Exception as e:
                print(f"   [SHEET-ERR] {e}")
        out_f.close()

    # ── Print Top 50 ──
    print(f"\n{'='*70}")
    print(f"[DONE] Enriched {enriched_count} brands. Results: {OUTPUT_FILE}")
    print(f"{'='*70}")

    # Re-read CSV and sort by lead score
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            all_enriched = sorted(reader, key=lambda r: int(r.get("lead_score", 0)), reverse=True)

        print(f"\n{'='*70}")
        print(f"TOP 50 HOT LEADS FOR BUSINESSONBOT")
        print(f"{'='*70}")
        print(f"{'#':<4} {'Brand':<30} {'Followers':<12} {'Score':<8} {'Ads?'}")
        print("-" * 70)
        for rank, r in enumerate(all_enriched[:50], 1):
            ads = "Yes" if "running" in r.get("ad_status", "").lower() else "No"
            print(f"{rank:<4} {r['brand_name'][:28]:<30} {r['followers']:<12} {r['lead_score']:<8} {ads}")


if __name__ == "__main__":
    main()
