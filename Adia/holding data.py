import pandas as pd
from models import engine


if __name__ == '__main__':

    my_sql = f"""SELECT YEAR(entry_date) AS year,MONTH(entry_date) AS month,MAX(entry_date) AS max_date
                FROM position GROUP BY YEAR(entry_date), MONTH(entry_date) ORDER BY year, month;"""
    df_date = pd.read_sql(my_sql, con=engine)
    # get all max date in a list
    # if last date is current month remove it
    df_date = df_date[:-1]
    max_date_list = df_date['max_date'].tolist()
    # get max_date_list in one string separated by comma
    max_date_list_str = ",".join(["'" + str(x) + "'" for x in max_date_list])

    my_sql = f"""SELECT T1.entry_date,T2.sedol,T2.name,'USD' as currency,mkt_value_usd,quantity FROM position T1 JOIN product T2 on t1.product_id=T2.id
    WHERE entry_date in ({max_date_list_str}) and parent_fund_id=1 and prod_type in ('Cash','Future') and entry_date>='2019-04-01'
    order by entry_date"""
    df_pos = pd.read_sql(my_sql, con=engine)

    # get sum mkt_value_usd when >0 grouped by entry_date
    df_long = df_pos[df_pos['mkt_value_usd'] > 0]
    df_long = df_long.groupby('entry_date')['mkt_value_usd'].sum().reset_index()
    # rename mkt_value_usd to long_usd
    df_long.rename(columns={'mkt_value_usd': 'long_usd'}, inplace=True)

    # merge df_pos and df_long
    df_pos = df_pos.merge(df_long, on='entry_date', how='left')
    df_pos['weight'] = df_pos['mkt_value_usd'] / df_pos['long_usd'] * 100
    df_pos['Price'] = df_pos['mkt_value_usd'] / df_pos['quantity']

    # reorganize columns: entry_date,sedol,name,currency,weight,mkt_value_usd,quantity,price
    df_pos = df_pos[['entry_date', 'sedol', 'name', 'currency', 'weight', 'mkt_value_usd', 'quantity', 'Price']]

    # order by entry_date, weight, desc
    df_pos.sort_values(by=['entry_date', 'weight'], ascending=[True, False], inplace=True)

    # save to excel
    df_pos.to_excel(r'Excel\adia_holding.xlsx', index=False)

    print(max_date_list_str)
