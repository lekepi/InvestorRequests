from datetime import date
from models import engine
import pandas as pd
from utils import find_past_date

def find_position(my_date):
    my_sql = f"""SELECT entry_date,T2.ticker,T2.sedol, T2.isin,T2.name as identifier,mkt_value_usd
    FROM position T1 JOIN product T2 on T1.product_id=T2.id
    WHERE entry_date='{my_date}' and prod_type in ('Cash', 'future') and parent_fund_id=1 
    order by mkt_value_usd desc;"""
    df_position = pd.read_sql(my_sql, con=engine)

    # get nav for that month
    my_sql = f"""SELECT amount/2*1000000 as aum FROM aum where type='leveraged' and entry_date <='{my_date}' 
    order by entry_date desc LIMIT 1;"""
    df_aum = pd.read_sql(my_sql, con=engine)
    aum = df_aum['aum'].values[0]

    df_position['allocation'] = df_position['mkt_value_usd'] / aum
    # remove mkt_value_usd
    df_position = df_position.drop(columns=['mkt_value_usd', 'entry_date'])

    return df_position


if __name__ == '__main__':
    my_date = date(2025, 6, 2)
    previous_date = find_past_date(my_date, 1)
    df_position = find_position(my_date)
    # save into excel
    df_position.to_excel(f'position_alto_{previous_date}.xlsx', index=False)
