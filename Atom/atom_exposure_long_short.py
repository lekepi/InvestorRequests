from datetime import date
import pandas as pd
import numpy as np
from models import engine


def classify_mkt_cap(x):
    if pd.isna(x):
        return "Unknown"
    elif x >= 20e9:
        return "Mega Cap (+20B)"
    elif x >= 10e9:
        return "Large Cap (10-20B)"
    elif x >= 2e9:
        return "Mid Cap (2-10B)"
    elif x >= 250e6:
        return "Small Cap (250MM-2B)"
    else:
        return "Micro Cap (<250MM)"


def get_ls_exposure(my_date):
    my_sql = f"""
        SELECT
            ABS(notional) AS notional_usd,
            prod_type,
            T4.name AS country,
            market_cap,
            T5.name AS sector
        FROM man_position T1
        JOIN product T2 ON T1.product_id = T2.id
        LEFT JOIN exchange T3 ON T2.exchange_id = T3.id
        JOIN country T4 ON T3.country_id = T4.id
        LEFT JOIN industry_group_gics T5 ON T2.industry_group_gics_id = T5.id
        WHERE entry_date = '{my_date}' AND prod_type = 'Cash'
    """

    df = pd.read_sql(my_sql, con=engine)

    # classify market cap
    df["mkt_cap_bucket"] = df["market_cap"].apply(classify_mkt_cap)

    # gross exposure
    gross = df["notional_usd"].sum()

    # ---------- helper ----------
    def build_table(group_col):
        out = (
            df.groupby(group_col)["notional_usd"]
                .sum()
                .reset_index()
        )
        out["pct_of_gross"] = out["notional_usd"] / gross
        out = out.sort_values("notional_usd", ascending=False)
        return out

    # 3 views
    mkt_cap_tab = build_table("mkt_cap_bucket")
    country_tab = build_table("country")
    sector_tab = build_table("sector")

    # ---------- Excel export ----------
    file_name = f"ls_exposure_{my_date}.xlsx"

    with pd.ExcelWriter(file_name, engine="xlsxwriter") as writer:
        mkt_cap_tab.to_excel(writer, sheet_name="Market Cap", index=False)
        country_tab.to_excel(writer, sheet_name="Country", index=False)
        sector_tab.to_excel(writer, sheet_name="Sector", index=False)

    print(f"Saved: {file_name} (gross={gross:,.0f})")


if __name__ == "__main__":
    my_date = date(2026, 6, 19)
    get_ls_exposure(my_date)