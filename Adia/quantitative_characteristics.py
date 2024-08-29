import pandas as pd
from models import engine
from datetime import date
import numpy as np


def get_quantitative_characteristics():
    my_sql = """SELECT entry_date,data_name,data_mtd/100 as alto_return FROM nav_account_statement WHERE active=1 and status='MonthEnd' 
and entry_date>='2019-04-01' and data_name='RETURN USD CLASS L' order by entry_date;"""
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    df['entry_date'] = df['entry_date'].dt.date
    df['entry_date'] = df['entry_date'].apply(lambda x: x.strftime('%Y-%m'))

    start = date(2019, 3, 1)
    end = date.today()
    date_range = pd.date_range(start, end, freq='BM')
    date_list = ["'" + d.strftime('%Y-%m-%d') + "'" for d in date_range]
    date_sql = ",".join(date_list)
    # put entry_date as index
    df.set_index('entry_date', inplace=True)

    my_sql = f"""SELECT entry_date,ticker,adj_price FROM product_market_data T1 JOIN product T2 on T1.product_id=T2.id 
                         WHERE ticker in ('SXXR Index', 'SPTR500N Index') and entry_date in ({date_sql})"""
    df_index = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    df_index['entry_date'] = df_index['entry_date'].dt.date
    df_index['entry_date'] = df_index['entry_date'].apply(lambda x: x.strftime('%Y-%m'))
    pivot_df_index = df_index.pivot(index='entry_date', values='adj_price', columns='ticker')
    pivot_df_index['SXXR Return'] = round(pivot_df_index['SXXR Index'].pct_change() * 100, 2)
    pivot_df_index['SPTR500N Return'] = round(pivot_df_index['SPTR500N Index'].pct_change() * 100, 2)
    pivot_df_index = pivot_df_index[['SXXR Return', 'SPTR500N Return']]
    # add benchmark return column that is 2/3 of SXXR Return  and 1/3 of SPTR500N Return
    pivot_df_index['Benchmark Return'] = ((pivot_df_index['SXXR Return'] * 2 + pivot_df_index['SPTR500N Return']) / 3) / 100
    pivot_df_index = pivot_df_index.iloc[1:]
    # merge both
    df = pd.merge(df, pivot_df_index, on='entry_date', how='left')
    # keep 'Alto Return' and 'Benchmark Return' only
    df = df[['alto_return', 'Benchmark Return']]
    # rename alto_return to 'Alto Return'
    df.rename(columns={'alto_return': 'Alto Return'}, inplace=True)
    df['Excess Return'] = df['Alto Return'] - df['Benchmark Return']

    periods = ['2023', '2022', '2021', '3Y', 'ITD']

    index_values = ['Annual Return', 'BM Annual Return', 'Excess Return', 'Beta', 'Information Ratio', 'Sharpe Ratio',
                    'Annual Std Dev', 'BM Annual Std Dev', 'Tracking Error']

    df_result = pd.DataFrame(columns=periods, index=index_values)


    for period in periods:
        if period in ['2023', '2022', '2021']:
            # keep when index start with 2021, 2022, 2023
            df_period = df[df.index.str.startswith(period)]
        elif period == '3Y':
            # keep the last 36 rows
            df_period = df.tail(36)
        elif period == 'ITD':
            # keep the last 60 rows
            df_period = df

        beta = np.polyfit(df_period['Benchmark Return'], df_period['Alto Return'], 1)[0]
        # complete df_result
        df_result.loc['Beta', period] = beta

        # annual return as product

        annual_return = (df_period['Alto Return'] + 1).prod() ** (12 / len(df_period)) - 1
        df_result.loc['Annual Return', period] = annual_return
        # benchmark annual return
        bm_annual_return = (df_period['Benchmark Return'] + 1).prod() ** (12 / len(df_period)) - 1
        df_result.loc['BM Annual Return', period] = bm_annual_return
        # excess return
        excess_return = (df_period['Excess Return'] + 1).prod() ** (12 / len(df_period)) - 1
        df_result.loc['Excess Return', period] = excess_return
        # tracking error
        tracking_error = df_period['Excess Return'].std() * np.sqrt(12)
        df_result.loc['Tracking Error', period] = tracking_error
        # annual std dev
        std_dev = df_period['Alto Return'].std() * np.sqrt(12)
        df_result.loc['Annual Std Dev', period] = std_dev
        # benchmark annual std dev
        bm_std_dev = df_period['Benchmark Return'].std() * np.sqrt(12)
        df_result.loc['BM Annual Std Dev', period] = bm_std_dev
        # information ratio
        df_result.loc['Information Ratio', period] = excess_return / tracking_error
        # sharpe ratio
        df_result.loc['Sharpe Ratio', period] = annual_return / std_dev

    # save in excel folder
    df_result.to_excel(f'excel/quantitative_characteristics.xlsx')


if __name__ == '__main__':
    get_quantitative_characteristics()
