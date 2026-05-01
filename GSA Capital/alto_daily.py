from datetime import date
from models import engine
import pandas as pd


# sent a first time on the 04-11-2025 until 2024-03-29
# sent a second time on the 06-03-2026 until 2026-03-02

def get_daily_alto_position(start_date, end_date):
    my_sql = f"""SELECT entry_date,quantity,T2.ticker,T2.isin,T2.sedol,T2.prod_type,mkt_value_usd as notional_usd FROM position T1 
JOIN product T2 on T1.product_id=T2.id WHERE entry_date>='{start_date}' and entry_date<='{end_date}' and 
parent_fund_id=1 and (prod_type='Cash' or T2.ticker in ('ES1 CME', 'SXO1 EUX', 'GC1 CMX')) order by entry_date;"""

    df_position = pd.read_sql(my_sql, con=engine)

    my_sql = """SELECT entry_date,round(amount*deployed,0)*10000 as aum FROM aum Where type='leveraged' and fund_id=4 and entry_date>='2019-04-01' order by entry_date;"""
    df_aum = pd.read_sql(my_sql, con=engine)
    df_position = pd.merge(df_position, df_aum, how='left', on='entry_date')
    df_position['aum'] = df_position['aum'].fillna(method='ffill')

    df_position['entry_date'] = pd.to_datetime(df_position['entry_date'])
    df_position['entry_date'] = df_position['entry_date'].dt.strftime('%Y-%m-%d')
    df_position.to_excel('Alto Daily Position.xlsx', index=False)


if __name__ == '__main__':
    start_date = date(2019, 4, 1)
    end_date = date(2026, 3, 2)
    get_daily_alto_position(start_date, end_date)
