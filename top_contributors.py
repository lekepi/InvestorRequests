import pandas as pd
from models import engine, config_class

def get_top_contributors():
    my_sql = """SELECT T2.ticker,T1.entry_date,T1.pnl_usd FROM position T1
JOIN Product T2 ON T1.product_id = T2.id WHERE prod_type = 'Cash' AND entry_date>='2019-04-01'
order by entry_date"""

    df_position = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    my_sql = """SELECT entry_date,amount*1000000 as amount FROM anandaprod.aum WHERE type='leveraged';"""
    df_aum = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    # merge
    df_temp = pd.merge(df_position, df_aum, on='entry_date', how='left')
    # fill with previous value
    df_temp['amount'] = df_temp['amount'].fillna(method='ffill')
    df_temp['perf'] = df_temp['pnl_usd'] / df_temp['amount']

    df_temp['month_year'] = df_temp['entry_date'].dt.strftime('%Y-%m')
    df = df_temp.groupby(['month_year', 'ticker'])['perf'].sum().reset_index()

    # sort by month_year and perf
    df = df.sort_values(by=['month_year', 'perf'], ascending=[True, False])

    # get all unique month_year
    month_year = df['month_year'].unique()
    # get top 5 contributors
    df_top_month = pd.DataFrame()
    df_bottom_month = pd.DataFrame()
    for my_month_year in month_year:
        df_current_month = df[df['month_year'] == my_month_year]

        df_top_5 = df_current_month.head(5)
        df_top_month[my_month_year] = df_top_5['ticker'].values

        df_bottom_5 = df_current_month.tail(5)
        df_bottom_month[my_month_year] = df_bottom_5['ticker'].values

    df_top_month.to_excel('Excel/Top 5 contributors by month.xlsx', index=False)
    df_bottom_month.to_excel('Excel/Top 5 detractors by month.xlsx', index=False)

    df_temp['year'] = df_temp['entry_date'].dt.strftime('%Y')
    df = df_temp.groupby(['year', 'ticker'])['perf'].sum().reset_index()
    df = df.sort_values(by=['year', 'perf'], ascending=[True, False])
    df_top_year = pd.DataFrame()
    df_bottom_year = pd.DataFrame()

    year = df['year'].unique()
    for my_year in year:
        df_top_5 = df[df['year'] == my_year].head(5)
        df_top_year[my_year] = df_top_5['ticker'].values

        df_bottom_5 = df[df['year'] == my_year].tail(5)
        df_bottom_year[my_year] = df_bottom_5['ticker'].values

    df_top_year.to_excel('Excel/Top 5 contributors by year.xlsx', index=False)
    df_bottom_year.to_excel('Excel/Top 5 detractors by year.xlsx', index=False)









    pass


if __name__ == '__main__':
    get_top_contributors()
    pass

