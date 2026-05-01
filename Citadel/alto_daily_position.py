from datetime import date
from models import engine
import pandas as pd


def get_daily_alto_position(start_date, end_date):
    my_sql = f"""SELECT entry_date,quantity,T2.ticker,T2.sedol,T2.prod_type,mkt_value_usd as notional_usd FROM position T1 
JOIN product T2 on T1.product_id=T2.id WHERE entry_date>='{start_date}' and entry_date<='{end_date}' and 
parent_fund_id=1 and (prod_type='Cash' or T2.ticker in ('ES1 CME', 'SXO1 EUX', 'GC1 CMX')) order by entry_date;"""

    df_position = pd.read_sql(my_sql, con=engine)
    # export df_position into the folder Citadel into excel
    df_position['entry_date'] = pd.to_datetime(df_position['entry_date'])
    df_position['entry_date'] = df_position['entry_date'].dt.strftime('%Y-%m-%d')
    df_position.to_excel('Alto Daily Position.xlsx', index=False)


if __name__ == '__main__':
    start_date = date(2019, 4, 1)
    end_date = date(2025, 10, 24)
    get_daily_alto_position(start_date, end_date)
