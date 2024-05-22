from models import engine
import pandas as pd


if __name__ == '__main__':
    my_sql = """SELECT entry_date,quantity,T2.ticker,T2.sedol,prod_type,mkt_value_usd as notional_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id 
WHERE entry_date>='2019-04-01' and entry_date<='2023-05-31' and parent_fund_id=1
and prod_type in ('Cash', 'Future') and T2.ticker not in ('TY1 CBT', 'GX1 EUX', 'ED1 CME', 'TY1 CBT', 'CF1 EOP', 'NQ1 CME')
order by entry_date;"""
    df_pos = pd.read_sql(my_sql, con=engine)
    # save into \Excel\position_history.xlsx
    df_pos.to_excel('Excel/position_history.xlsx', index=False)
