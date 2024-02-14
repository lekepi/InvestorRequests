import pandas as pd
from models import engine

"""
get the minimum market cap own by region EMEA, AMER
"""

def fill_market_cap(df):
    for i in range(1, len(df)):
        if pd.isnull(df.loc[i, 'market_cap']) and df.loc[i, 'ticker'] == df.loc[i-1, 'ticker']:
            df.loc[i, 'market_cap'] = df.loc[i-1, 'market_cap']
    return df


def get_min_market_cap():

    my_sql = """SELECT min(entry_date) as min_date,T2.ticker FROM anandaprod.position T1 JOIN product T2 on T1.product_id=T2.id
WHERE entry_date>='2019-04-01' and parent_fund_id=1 and prod_type='Cash' group by T2.ticker,year(entry_date),month(entry_date);"""

    df_position = pd.read_sql(my_sql, con=engine, parse_dates=['min_date'])
    df_position['year_month'] = df_position['min_date'].dt.strftime('%Y-%m')

    df_position['continent'] = 'EMEA'
    df_position.loc[df_position['ticker'].str.contains(" U"), 'continent'] = 'AMER'
    df_position.loc[df_position['ticker'].str.contains(" CN"), 'continent'] = 'AMER'

    my_sql = """SELECT T1.entry_date,T2.ticker,T1.market_cap from product_market_cap T1 
    JOIN product T2 on T1.product_id=T2.id WHERE T1.type='Monthly'"""

    df_market_cap = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    df_market_cap['year_month'] = df_market_cap['entry_date'].dt.strftime('%Y-%m')

    df = pd.merge(df_position, df_market_cap, on=['ticker', 'year_month'], how='left')
    # sort df by the column market_cap asc only
    df = df.sort_values(by='market_cap', ascending=True)
    df_amer = df[df['continent'] == 'AMER']
    df_emea = df[df['continent'] == 'EMEA']

    df_amer_5 = df_amer.groupby('year_month').head(5)
    df_emea_5 = df_emea.groupby('year_month').head(5)

    pass


if __name__ == '__main__':
    get_min_market_cap()


