import csv
with open('lead_enrichment_results.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    print("Brands with Score < 15 or 0 followers:")
    for r in reader:
        score = int(r.get('lead_score', 0))
        foll = int(r.get('followers', 0) or 0)
        likes = int(r.get('likes', 0) or 0)
        if score < 15 or foll == 0 or likes == 0:
            print(f"- {r['brand_name']}: Score={score}, Followers={foll}, Likes={likes}")
