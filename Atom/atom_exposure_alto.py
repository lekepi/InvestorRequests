from models import engine
import pandas as pd
from datetime import date
import numpy as np


def classify_mkt_cap(x):
    if pd.isna(x):
        return np.nan
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


def get_average_exposure():

    today = date.today()
    last_date = today.replace(day=1)

    my_sql = f"""SELECT MAX(entry_date) AS month_end_date
                FROM position where entry_date>'2019-04-01' and entry_date<'{last_date}'
                GROUP BY DATE_FORMAT(entry_date, '%%Y-%%m');"""
    df_date = pd.read_sql(my_sql, con=engine)
    date_list = df_date['month_end_date'].tolist()
    date_sql = ",".join([f"'{d}'" for d in date_list])

    my_sql = f"""SELECT entry_date,abs(mkt_value_usd) as notional_usd,prod_type,T4.name as country,market_cap,T5.name as sector FROM position 
    T1 JOIN product T2 on T1.product_id=T2.id
    JOIN exchange T3 on T2.exchange_id=T3.id JOIN country T4 on T3.country_id=T4.id
    JOIN industry_group_gics T5 on T2.industry_group_gics_id=T5.id
    WHERE parent_fund_id=1 and entry_date in ({date_sql});"""

    df_position = pd.read_sql(my_sql, con=engine)

    df_mk = df_position[['entry_date', 'notional_usd', 'market_cap']]
    df_mk = df_mk[df_mk['market_cap'].notnull()]
    df_mk["cap_bucket"] = df_mk["market_cap"].apply(classify_mkt_cap)

    df_mk_exposure = (
        df_mk.groupby(["entry_date", "cap_bucket"])["notional_usd"]
        .sum()
        .reset_index()
    )

    df_mk_exposure["total_date"] = df_mk_exposure.groupby("entry_date")["notional_usd"].transform("sum")
    df_mk_exposure["exposure_pct"] = df_mk_exposure["notional_usd"] / df_mk_exposure["total_date"]

    all_dates = df_mk_exposure["entry_date"].unique()
    all_caps = df_mk_exposure["cap_bucket"].unique()

    idx = pd.MultiIndex.from_product(
        [all_dates, all_caps],
        names=["entry_date", "cap_bucket"]
    )

    df_mk_full = (
        df_mk_exposure
        .set_index(["entry_date", "cap_bucket"])
        .reindex(idx, fill_value=0)
        .reset_index()
    )

    df_mk_avg = (
        df_mk_full
        .groupby("cap_bucket")["exposure_pct"]
        .mean()
        .reset_index()
        .rename(columns={"exposure_pct": "avg_exposure_pct"})
    )

    df_mk_avg.to_excel('Average_Exposure_by_Market_Cap.xlsx', index=False)

    df_ct = df_position[['entry_date', 'notional_usd', 'country']]
    df_ct = df_ct[df_ct['country'].notnull()]

    df_ct_exposure = (
        df_ct.groupby(["entry_date", "country"])["notional_usd"]
        .sum()
        .reset_index()
    )

    df_ct_exposure["total_date"] = df_ct_exposure.groupby("entry_date")["notional_usd"].transform("sum")
    df_ct_exposure["exposure_pct"] = df_ct_exposure["notional_usd"] / df_ct_exposure["total_date"]

    all_dates = df_ct_exposure["entry_date"].unique()
    all_countries = df_ct_exposure["country"].unique()

    idx = pd.MultiIndex.from_product(
        [all_dates, all_countries],
        names=["entry_date", "country"]
    )

    df_ct_full = (
        df_ct_exposure
        .set_index(["entry_date", "country"])
        .reindex(idx, fill_value=0)
        .reset_index()
    )

    df_country_avg = (
        df_ct_full
        .groupby("country")["exposure_pct"]
        .mean()
        .reset_index()
        .rename(columns={"exposure_pct": "avg_exposure_pct"})
    )

    df_country_avg.to_excel('Average_Exposure_by_Country.xlsx', index=False)

    df_sec = df_position[['entry_date', 'notional_usd', 'sector']]
    df_sec = df_sec[df_sec['sector'].notnull()]

    df_sec_exposure = (
        df_sec.groupby(["entry_date", "sector"])["notional_usd"]
        .sum()
        .reset_index()
    )

    df_sec_exposure["total_date"] = df_sec_exposure.groupby("entry_date")["notional_usd"].transform("sum")
    df_sec_exposure["exposure_pct"] = df_sec_exposure["notional_usd"] / df_sec_exposure["total_date"]

    all_dates = df_sec_exposure["entry_date"].unique()
    all_sectors = df_sec_exposure["sector"].unique()

    idx = pd.MultiIndex.from_product(
        [all_dates, all_sectors],
        names=["entry_date", "sector"]
    )

    df_sec_full = (
        df_sec_exposure
        .set_index(["entry_date", "sector"])
        .reindex(idx, fill_value=0)
        .reset_index()
    )

    df_sector_avg = (
        df_sec_full
        .groupby("sector")["exposure_pct"]
        .mean()
        .reset_index()
        .rename(columns={"exposure_pct": "avg_exposure_pct"})
    )

    df_sector_avg.to_excel('Average_Exposure_by_Sector.xlsx', index=False)


if __name__ == "__main__":
    get_average_exposure()
