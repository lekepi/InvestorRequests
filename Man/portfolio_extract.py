import pandas as pd
from bs4 import BeautifulSoup

# Load HTML
with open(r"H:\Investor\Man\HTML\order_history_html.txt", "r", encoding="utf-8") as f:
    html = f.read()

soup = BeautifulSoup(html, "html.parser")

rows_data = []

# Find the table container after the "ag-pinned-right-header"
start_div = soup.find("div", class_="ag-pinned-right-header")
if not start_div:
    raise ValueError("Could not find starting div for table.")

table_container = start_div.find_next("div", class_="ag-center-cols-container")
if not table_container:
    raise ValueError("Could not find table container after starting div.")

# Loop through all rows
for row in table_container.find_all("div", class_="ag-row"):
    order_id = row.find("div", {"col-id": "orderBasketId"})
    submitted = row.find("div", {"col-id": "createdAt"})
    side_span = row.find("div", {"col-id": "side"})
    side = side_span.get_text(strip=True) if side_span else None
    ticker_div = row.find("div", {"col-id": "bbgTickExch"})
    ticker = ticker_div.get_text(strip=True) if ticker_div else None
    notional_div = row.find("div", {"col-id": "notionalUsd"})
    notional = notional_div.get_text(strip=True) if notional_div else None
    status_div = row.find("div", {"col-id": "status"})
    status_span = status_div.find("span") if status_div else None
    status = status_span.get_text(strip=True) if status_span else None

    # Append row data
    rows_data.append({
        "Order ID": order_id.get_text(strip=True) if order_id else None,
        "Submitted": submitted.get_text(strip=True) if submitted else None,
        "Side": side,
        "Ticker": ticker,
        "Notional $": notional,
        "Status": status
    })

# Convert to DataFrame
df = pd.DataFrame(rows_data)

# Optional: clean Notional $ column
df["Notional $"] = df["Notional $"].str.replace("[$,]", "", regex=True).astype(float)
df["Submitted"] = pd.to_datetime(df["Submitted"], format="%d/%m/%y %H:%M:%S", errors="coerce")

print(df)
