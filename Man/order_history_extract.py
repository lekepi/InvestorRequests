from bs4 import BeautifulSoup
import pandas as pd

# Load HTML
with open(r"H:\Investor\Man\HTML\order_history_html.txt", "r", encoding="utf-8") as f:
    html = f.read()

soup = BeautifulSoup(html, "html.parser")

# Find all row divs that have a row-id
row_divs = soup.find_all("div", {"role": "row", "row-id": True})

# Prepare list to store row data
all_rows = []

for row in row_divs:
    row_data = {}

    # Save row-id
    row_data["row-id"] = row.get("row-id")

    # Iterate over all columns in this row (divs with col-id)
    col_divs = row.find_all("div", {"col-id": True})
    for col in col_divs:
        col_id = col["col-id"]
        # Check if it contains a span for the value (like side or status)
        span = col.find("span")
        value = span.get_text(strip=True) if span else col.get_text(strip=True)
        row_data[col_id] = value

    all_rows.append(row_data)

# Convert to DataFrame
df = pd.DataFrame(all_rows)

# Remove rows where Ticker column is literally 'Ticker' (if exists)
if "bbgTickExch" in df.columns:
    df = df[df["bbgTickExch"] != "Ticker"]

# Clean Notional $ if present
if "notionalUsd" in df.columns:
    df["notionalUsd"] = df["notionalUsd"].str.replace("[$,]", "", regex=True).astype(float)

# Convert Submitted / createdAt to datetime if present
if "createdAt" in df.columns:
    df["createdAt"] = pd.to_datetime(df["createdAt"], format="%d/%m/%y %H:%M:%S", errors="coerce")

print(df)
