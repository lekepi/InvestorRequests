import pandas as pd
from models import engine, session, Product, ExecFee, TaskChecker


"""
Get the biggest position on single stock (long and Short) in % of the AUM(fund) for each day
13% since inception for long, 8% since 2022...

Then get the average long and short position % of AUM_fund for the core position (top30) since inception
"""

def get_position_size():

    my_sql = "SELECT entry_date,amount as aum FROM aum WHERE type='fund' and entry_date>='2019-03-28' order by entry_date;"
    df_aum = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    # for index=0, change entry_date to 2019-03-28
    df_aum.loc[0, 'entry_date'] = pd.to_datetime('2019-04-01')

    my_sql = """SELECT entry_date,T2.ticker,mkt_value_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id WHERE prod_type='Cash' and parent_fund_id=1
and entry_date>='2019-04-01' and mkt_value_usd is Not NULL order by entry_date;"""
    df_position = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    df_position = pd.merge(df_position, df_aum, on='entry_date', how='left')
    # fill aum with the last available value
    df_position['aum'] = df_position['aum'].fillna(method='ffill')
    df_position['position_size'] = df_position['mkt_value_usd'] / df_position['aum']

    df_position_long = df_position[df_position['mkt_value_usd'] > 0]
    df_position_short = df_position[df_position['mkt_value_usd'] < 0]

    # sort df_position_long by 'position_size' in descending order
    df_position_long = df_position_long.sort_values(by='position_size', ascending=False)
    # sort df_position_short by 'position_size' in ascending order
    df_position_short = df_position_short.sort_values(by='position_size', ascending=True)

    # save df_position_long into excel xlsx file
    df_position_long.to_excel('Excel/position_size_long.xlsx', index=False)
    # save df_position_short into excel xlsx file
    df_position_short.to_excel('Excel/position_size_short.xlsx', index=False)

    # get average of core position
    core_position = 30

    df_long_avg = df_position_long.sort_values(by=['entry_date', 'position_size'], ascending=[True, False])
    result = df_long_avg.groupby('entry_date').head(core_position).groupby('entry_date')['position_size'].mean().reset_index()
    # save result into excel xlsx file
    result.to_excel('Excel/position_size_long_avg.xlsx', index=False)

    df_short_avg = df_position_short.sort_values(by=['entry_date', 'position_size'], ascending=[True, True])
    result = df_short_avg.groupby('entry_date').head(core_position).groupby('entry_date')['position_size'].mean().reset_index()
    # save result into excel xlsx file
    result.to_excel('Excel/position_size_short_avg.xlsx', index=False)


if __name__ == '__main__':
    get_position_size()
