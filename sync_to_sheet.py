import csv
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Config
CSV_FILE = "lead_enrichment_results.csv"
CREDS_FILE = "ticket-automation-478816-065d3bcabe7e.json"
SHEET_ID = "1epekP6pptvzWpkEI-5-nsXv9iR8M4zX8QsOb5SfrZag"
SHEET_TAB = "All-Leads"

# Enrichment Columns mapping (Header on sheet : Key in CSV)
COL_MAPPING = {
    "FB Followers": "followers",
    "FB Likes": "likes",
    "FB Category": "category",
    "FB Email": "email",
    "FB Phone": "phone",
    "FB Ad Status": "ad_status",
    "FB Posts/30d": "posts_last_30d",
    "Avg Engagement": "avg_engagement", # Note: CSV has avg_likes etc, we'll calculate
    "Last Post Date": "last_post_date",
    "Lead Score": "lead_score"
}

def sync():
    if not os.path.exists(CSV_FILE):
        print(f"[ERR] {CSV_FILE} not found.")
        return

    # 1. Load CSV data
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        csv_data = {row["brand_name"]: row for row in reader}

    # 2. Authenticate
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_TAB)

    # 3. Get Sheet headers and rows
    all_rows = sheet.get_all_records()
    headers = sheet.row_values(1)

    # Find column indices for enrichment headers
    col_indices = {}
    for h in COL_MAPPING.keys():
        if h in headers:
            col_indices[h] = headers.index(h) + 1
        else:
            print(f"[ERR] Column '{h}' not found in sheet.")
            return

    brand_col_idx = headers.index("Brand Name") + 1

    updates = []
    updated_count = 0

    # 4. Process each row in the sheet
    for i, row in enumerate(all_rows, 2):
        brand_name = row.get("Brand Name")
        if brand_name in csv_data:
            c_row = csv_data[brand_name]
            
            # Prepare updates for this row
            for h, csv_key in COL_MAPPING.items():
                val = ""
                if h == "Avg Engagement":
                    try:
                        val = float(c_row.get("avg_likes", 0)) + float(c_row.get("avg_shares", 0)) + float(c_row.get("avg_reactions", 0))
                    except: val = 0
                else:
                    val = c_row.get(csv_key, "")

                updates.append({
                    "range": gspread.utils.rowcol_to_a1(i, col_indices[h]),
                    "values": [[val]]
                })
            updated_count += 1

    # 5. Push batch updates
    if updates:
        print(f"[INFO] Pushing {len(updates)} cell updates for {updated_count} brands...")
        sheet.batch_update(updates)
        print("[OK] Sync complete.")

if __name__ == "__main__":
    sync()
