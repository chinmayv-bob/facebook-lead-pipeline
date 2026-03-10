import csv
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Config
CSV_FILE = "lead_enrichment_results.csv"
CREDS_FILE = "ticket-automation-478816-065d3bcabe7e.json"
SHEET_ID = "1epekP6pptvzWpkEI-5-nsXv9iR8M4zX8QsOb5SfrZag"
NEW_TAB_NAME = "Top 50 Hot Leads"

def export_top_50():
    if not os.path.exists(CSV_FILE):
        print(f"[ERR] {CSV_FILE} not found.")
        return

    # 1. Load and Sort CSV data
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        all_data = list(reader)
    
    # Sort by lead_score descending
    # Need to handle potential non-integer scores
    def get_score(row):
        try:
            return int(row.get("lead_score", 0))
        except:
            return 0

    sorted_leads = sorted(all_data, key=get_score, reverse=True)
    top_50 = sorted_leads[:50]

    # 2. Authenticate
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
    client = gspread.authorize(creds)
    ss = client.open_by_key(SHEET_ID)

    # 3. Create or clear the new tab
    try:
        worksheet = ss.worksheet(NEW_TAB_NAME)
        ss.del_worksheet(worksheet)
        print(f"[INFO] Deleted existing '{NEW_TAB_NAME}' tab.")
    except gspread.exceptions.WorksheetNotFound:
        pass
    
    # Create new worksheet with 51 rows (header + 50) and number of columns in top_50[0]
    # 4. Prepare data for writing
    headers = ["#", "Brand", "Followers", "Score", "Ads?"]
    worksheet = ss.add_worksheet(title=NEW_TAB_NAME, rows=100, cols=len(headers))
    print(f"[OK] Created new tab: '{NEW_TAB_NAME}'")

    data_to_write = [headers]
    
    for i, row in enumerate(top_50, 1):
        ads = "Yes" if "running" in row.get("ad_status", "").lower() else "No"
        data_to_write.append([
            i, 
            row.get("brand_name", ""),
            row.get("followers", ""),
            row.get("lead_score", ""),
            ads
        ])

    # 5. Write to sheet
    worksheet.update("A1", data_to_write)
    print(f"[OK] Successfully wrote filtered top 50 leads to '{NEW_TAB_NAME}'.")

if __name__ == "__main__":
    export_top_50()
