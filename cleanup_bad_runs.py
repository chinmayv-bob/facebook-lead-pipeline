import csv
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

CSV_FILE = "lead_enrichment_results.csv"
CREDS_FILE = "ticket-automation-478816-065d3bcabe7e.json"
SHEET_ID = "1epekP6pptvzWpkEI-5-nsXv9iR8M4zX8QsOb5SfrZag"
SHEET_TAB = "Sheet9"

def cleanup():
    # 1. Open CSV and filter out the bad rows
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        all_rows = list(reader)

    good_rows = []
    bad_brands = []

    for row in all_rows:
        # Check if it was an empty fallback row (0 score, 0 followers) or a known timeout
        # Exception for baomee / mindnutrition which legitimately have 0 followers but ran fully
        score = int(row.get('lead_score', 0))
        foll = int(row.get('followers', 0) or 0)
        
        # Brands that failed API completely
        failed_api_brands = [
            "enhancedlabs", "nb", "citizenwatches", "Tahska", "anisue",
            "stellardrive", "goalifynutrition", "flauntyourink", "damehealth",
            "twobrothersfood", "theyardhouse", "partypropz", "thealtbeauty", "bringmyflowers"
        ]

        if row['brand_name'] in failed_api_brands:
            bad_brands.append(row['brand_name'])
        else:
            good_rows.append(row)

    print(f"Filtering out {len(bad_brands)} failed brands from CSV...")

    # 2. Rewrite clean CSV
    with open(CSV_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(good_rows)

    # 3. Clean up Google Sheet
    print("Clearing failed brands from Google Sheet...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_TAB)

    sheet_rows = sheet.get_all_records()
    sheet_headers = sheet.row_values(1)
    
    # Columns to clear (start index to end)
    start_col_idx = sheet_headers.index("FB Followers") + 1
    end_col_idx = sheet_headers.index("Lead Score") + 1

    updates = []
    for i, row in enumerate(sheet_rows, 2):
        if row.get("Brand Name") in bad_brands:
            # Create empty cell updates for columns 4 through 13
            for col in range(start_col_idx, end_col_idx + 1):
                updates.append({
                    "range": gspread.utils.rowcol_to_a1(i, col),
                    "values": [[""]]
                })

    if updates:
        print(f"Pushing {len(updates)} cell clearing updates to Sheet...")
        sheet.batch_update(updates)
    
    print("Cleanup complete!")

if __name__ == "__main__":
    cleanup()
