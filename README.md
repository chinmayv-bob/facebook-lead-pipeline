# Facebook Lead Pipeline

Pipeline to discover Facebook pages from a brand list, enrich them with Apify data, score lead quality, and sync results back to Google Sheets.

Built as part of a hackathon while we were adding Messenger as a channel for the support inbox.

This repo contains a set of standalone Python scripts used by BusinessOnBot to:
1. Find Facebook profile URLs for brands.
2. Enrich those pages with profile + recent post data.
3. Score and prioritize leads.
4. Write enriched data back to a Google Sheet and export a Top 50 list.

## What It Does
- **Finds FB pages** from website or Shopify URLs (`facebook_extractor.py`)
- **Enriches** with Apify profile + posts data (`lead_enrichment.py`)
- **Scores** leads on a 100-point rubric (see `logic.md`)
- **Syncs** to Google Sheets + exports a Top 50 tab

## Quick Start
### 1) Prereqs
- Python 3.9+ recommended
- Access to the target Google Sheet
- A Google service account JSON key file
- An Apify API token

### 2) Install dependencies
```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install requests beautifulsoup4 gspread oauth2client
```

### 3) Configure secrets
- Place your Google service account key at `ticket-automation-478816-065d3bcabe7e.json` (or update `CREDS_FILE` in scripts).
- Set your Apify token:

PowerShell:
```bash
$env:APIFY_TOKEN="your_apify_token"
```
CMD:
```bash
set APIFY_TOKEN=your_apify_token
```
macOS/Linux:
```bash
export APIFY_TOKEN=your_apify_token
```

## Pipeline Usage
### 1) Discover Facebook URLs
Pulls a Google Sheet (Sheet9), scans each brand site for a Facebook link, and writes results:
```bash
python facebook_extractor.py
```
Outputs a local CSV (`facebook_scrape_results_sheet9.csv`) and optionally writes back to the sheet.

### 2) Enrich + Score Leads
Fetches profile + recent posts data from Apify and scores each lead:
```bash
python lead_enrichment.py
```
Outputs:
- `lead_enrichment_results.csv` (scored output)
- `lead_enrichment_raw.json` (raw Apify payloads)

### 3) Export Top 50
Creates a new sheet tab with the top 50 scored leads:
```bash
python export_top_50.py
```

### 4) Sync CSV Back to Sheet (optional)
If you enriched offline and want to push results back:
```bash
python sync_to_sheet.py
```

### 5) Migrate FB Profiles Between Tabs (optional)
Copies Facebook Profile values from Sheet1 to Sheet9:
```bash
python migrate_sheet_data.py
```

## Lead Scoring
The 100-point rubric weights:
- Audience size (followers)
- Ad activity (running ads)
- Posting cadence (last 30 days)
- Average engagement per post
- Contactability (email/phone)

Full scoring logic lives in `logic.md`.

## Configuration
Each script has a **CONFIG** section at the top. Common fields:
- `SHEET_ID`, `GID`, `SHEET_TAB`
- `CREDS_FILE` (service account JSON)
- `APIFY_TOKEN` (env var, used in `lead_enrichment.py`)

## Data + Secrets
Sensitive files and one-off output data are ignored by default:
- `*.csv`, `*_raw.json`, and the service account JSON are in `.gitignore`.
- Do **not** commit secrets; use env vars and local files.

## Notes / Troubleshooting
- If Sheets auth fails, scripts will still write local CSVs.
- If Apify credits are exhausted, `lead_enrichment.py` stops safely and can be rerun.
- The scraper is polite by default (rate limits + retry logic); adjust delays only if needed.

## Compliance
Make sure your data collection respects the target platform’s terms of service and any applicable privacy regulations.
