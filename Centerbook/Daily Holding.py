import pandas as pd
from models import engine


if __name__ == '__main__':

    start_date = '2019-04-01'
    end_date = '2024-12-31'

    my_sql = f"""SELECT T1.entry_date,T2.ticker as bbg_ticker,T2.sedol,T2.name,quantity,'USD' as currency,mkt_value_usd FROM position T1 JOIN product T2 on t1.product_id=T2.id
    WHERE parent_fund_id=1 and (prod_type='Cash' or T2.ticker in ('SXO1 EUX', 'ES1 CME')) and entry_date>='{start_date}' and entry_date<= '{end_date}' 
    and mkt_value_usd is not NULL order by entry_date,mkt_value_usd desc;"""
    df_pos = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    my_sql = f"""SELECT entry_date,amount*deployed/100 * 1000000/2 as nav FROM aum WHERE type='leveraged' and fund_id=4 
    and entry_date>='{start_date}' and entry_date<= '{end_date}' order by entry_date;"""
    df_nav = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    df_pos = df_pos.merge(df_nav, on='entry_date', how='left')
    # fill nav with previous value
    df_pos['nav'] = df_pos['nav'].fillna(method='ffill')

    df_pos['expo/nav'] = df_pos['mkt_value_usd'] / df_pos['nav']

    # save into excel folder
    df_pos.to_excel(r'Excel\Centerbook_holding.xlsx', index=False)

    pass
