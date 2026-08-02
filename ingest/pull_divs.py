import os, sys, json, urllib.request
KEY = os.environ["MASSIVE_API_KEY"]
url = ("https://api.massive.com/v3/reference/dividends"
       f"?ex_dividend_date.gte={sys.argv[1]}&ex_dividend_date.lte={sys.argv[2]}"
       f"&limit=1000&apiKey={KEY}")
rows = []
while url:
    with urllib.request.urlopen(url) as r:
        d = json.load(r)
    rows += d.get("results", [])
    nxt = d.get("next_url")
    url = f"{nxt}&apiKey={KEY}" if nxt else None
    print(len(rows), "rows", flush=True)
with open(sys.argv[3], "w") as f:
    for x in rows:
        f.write(json.dumps(x) + "\n")
print("wrote", len(rows))
