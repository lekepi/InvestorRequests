from datetime import date
import pandas as pd
from models import engine
import numpy as np

def get_market_cap():
    date_list = [date(2021, 12, 31), date(2022, 12, 30), date(2023, 12, 29), date(2024, 6, 28)]
    values = ['<$10 bn', '$10-50 bn', '$50-120 bn', '>$120 bn']

    df_result = pd.DataFrame(columns=date_list, index=values)

    for my_date in date_list:

        print(my_date)

        my_sql = f"""SELECT T2.ticker,abs(mkt_value_usd) as notional FROM position T1 JOIN product T2 on T1.product_id=T2.id 
        WHERE parent_fund_id=1 and prod_type='Cash' and entry_date='{my_date}';"""
        df = pd.read_sql(my_sql, con=engine)

        # get market cap
        my_sql = f"""SELECT max(entry_date) FROM product_market_cap WHERE type='Monthly' and entry_date<='{my_date}';"""
        last_date = pd.read_sql(my_sql, con=engine).iloc[0, 0].date()

        my_sql = f"""SELECT T2.ticker,T1.market_cap FROM product_market_cap T1 JOIN product T2 on T1.product_id=T2.id
        WHERE type='Monthly' and entry_date='{last_date}';"""
        df_market_cap = pd.read_sql(my_sql, con=engine)

        df = pd.merge(df, df_market_cap, on=['ticker'], how='left')
        # remove rows with NaN
        df = df.dropna()

        # get notional % of total notional
        df['percent'] = df['notional'] / df['notional'].sum() * 100

        conditions = [
            (df['market_cap'] < 10000),
            (df['market_cap'] >= 10000) & (df['market_cap'] < 50000),
            (df['market_cap'] >= 50000) & (df['market_cap'] < 120000),
            (df['market_cap'] >= 120000)
        ]

        # Create a new column based on the conditions
        df['market_cap_type'] = np.select(conditions, values)

        # get the percentage of notional for each market cap type
        df_group = df.groupby(['market_cap_type'])['percent'].sum().reset_index()

        for index, row in df_group.iterrows():
            df_result.loc[row['market_cap_type'], my_date] = round(row['percent'], 2)

    # save in excel folder
    df_result.to_excel('excel/market_cap.xlsx', index=True)


if __name__ == '__main__':
    get_market_cap()
