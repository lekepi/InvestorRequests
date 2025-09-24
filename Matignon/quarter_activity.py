# Get the net position change per week
from datetime import date
import pandas as pd
from models import engine


if __name__ == '__main__':
    start_date = date(2025, 4, 1)

    my_sql = f"""SELECT trade_date,T2.ticker,quantity,notional_usd FROM trade T1 JOIN product T2 on T1.product_id=T2.id
     WHERE trade_date>='2025-03-31' and trade_date<'2025-06-30' and position_side='Long' and parent_fund_id=1 
     and prod_type='cash' order by trade_date,ticker;"""
    df_trades = pd.read_sql(my_sql, con=engine)
    df_trades['trade_date'] = pd.to_datetime(df_trades['trade_date'])
    df_trades['week_monday'] = df_trades['trade_date'] - pd.to_timedelta(df_trades['trade_date'].dt.weekday, unit='d')

    df_weekly = df_trades.groupby(['week_monday', 'ticker'], as_index=False).agg({'quantity': 'sum', 'notional_usd': 'sum'})
    df_weekly['week_monday'] = df_weekly['week_monday'].dt.strftime('%Y-%m-%d')
    df_weekly.to_excel('quarter_activity Q2-2025.xlsx', index=False)
