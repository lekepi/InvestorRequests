
import pandas as pd
from models import engine
import numpy as np


if __name__ == '__main__':
    # get pnl
    my_sql = "SELECT entry_date,sum(pnl_usd) as pnl FROM position WHERE entry_date>='2019-04-01'" \
             " and parent_fund_id=1 group by entry_date order by entry_date;"
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col=['entry_date'])
    # get AUM
    my_sql = "Select entry_date,amount*deployed/100 as deployed_aum from aum where entry_date>='2019-04-01' and type='leveraged' and fund_id=4 order by entry_date"
    df_aum = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col=['entry_date'])

    # merge pnl and aum
    df = df.merge(df_aum, left_index=True, right_index=True, how='outer')

    # get the list of all distinct year# fill df['aum'] with the last valid value
    df['deployed_aum'].fillna(method='ffill', inplace=True)
    df['daily_return'] = df['pnl'] / (df['deployed_aum'] * 1000000) * 2  # for Alto, 2 is the leverage

    # 261 days in a year
    # calculate the standard dev for 261 days multiplied by Squareroot of 261

    # reset index
    df.reset_index(inplace=True)

    df['std_dev'] = df['daily_return'].rolling(261, min_periods=10).std()
    df['mean'] = df['daily_return'].rolling(261, min_periods=10).mean()
    df['Volatility'] = df['std_dev'] * np.minimum(df.index, 261) ** 0.5
    df['VAR'] = df['daily_return'].rolling(261, min_periods=10).quantile(0.05)
    df['CVAR'] = df['daily_return'].rolling(261, min_periods=10).apply(lambda x: x[x <= x.quantile(0.05)].mean())

    # put entry_date as index
    df.set_index('entry_date', inplace=True)
    # keep last available date of each month
    df_final = df.resample('M').last()

    # save to excel
    df_final.to_excel('daily_return.xlsx')

    pass