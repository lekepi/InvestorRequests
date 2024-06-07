import pandas as pd
from models import engine
import os


def get_turn_over():
    my_sql = """SELECT YEAR(trade_date) AS year, SUM(CASE WHEN notional_usd > 0 THEN notional_usd ELSE 0 END) AS buy_sum,
    SUM(CASE WHEN notional_usd < 0 THEN notional_usd ELSE 0 END) AS sell_sum FROM trade T1 JOIN product T2 
    on T1.product_id=T2.id WHERE parent_fund_id=1 and prod_type='cash' and trade_date>='2019-04-01' group by YEAR(trade_date);"""

    df_buy_sell = pd.read_sql(my_sql, con=engine, index_col='year')

    my_sql = "SELECT year(entry_date) as year, avg(amount) as amount FROM aum where type='Leveraged' and fund_id=4 group by year(entry_date);"
    df_aum = pd.read_sql(my_sql, con=engine, index_col='year')

    df = df_buy_sell.join(df_aum, how='outer')
    df['sell_sum'] = df['sell_sum'].abs()

    df['Min'] = df[['buy_sum', 'sell_sum']].min(axis=1)
    df['Turnover'] = df['Min'] / df['amount'] / 1000000

    df.to_excel(r'Excel\adia_turnover.xlsx', index=True)


if __name__ == '__main__':

    get_turn_over()
    pass