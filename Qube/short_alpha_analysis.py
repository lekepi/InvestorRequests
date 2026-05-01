
import pandas as pd
from models import engine
from datetime import datetime, timedelta


def get_df_alpha(start_date, end_date, alpha_pnl='alpha'):
    # Tables: get alpha BP, USD in a table per stock - 1D,1W,1M,3M,6M,1Y
    # alpha USD big DF with a column per stock

    my_sql = f"""SELECT T2.ticker,T1.entry_date,T1.{alpha_pnl}_usd as USD,T1.product_id FROM position T1
    JOIN product T2 on T1.product_id=T2.id WHERE parent_fund_id=1 and entry_date>='{start_date}' 
    and T1.quantity<>0 and entry_date<='{end_date}' AND T2.prod_type = 'Cash' Order by T2.ticker, T1.entry_date;"""
    df_alpha_usd = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    # Long notional USD
    my_sql = f"""SELECT T1.entry_date,sum(T1.mkt_value_usd) as notional_usd FROM position T1
    JOIN product T2 on T1.product_id=T2.id WHERE T2.prod_type = 'Cash' and T1.quantity>0
    and parent_fund_id=1 and entry_date>='{start_date}' and entry_date<='{end_date}' group by T1.entry_date
    Order by T1.entry_date;"""
    df_long_usd = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    df_alpha = df_alpha_usd.join(df_long_usd)
    df_alpha['BP'] = df_alpha['USD'] / df_alpha['notional_usd'] * 10000

    df_alpha.drop(columns=['notional_usd'], inplace=True)

    # all
    df_alpha_merge = df_alpha.groupby(['ticker'], as_index=False)[['USD', 'BP']].sum()
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')

    df_alpha_2019 = df_alpha['2019-01-01':'2019-12-31']
    df_alpha_2019_group = df_alpha_2019.groupby(['ticker'], as_index=False)[['USD', 'BP']].sum()
    df_alpha_2019_group.columns = ['ticker', 'USD 2019', 'BP 2019']
    df_alpha_2020 = df_alpha['2020-01-01':'2020-12-31']
    df_alpha_2020_group = df_alpha_2020.groupby(['ticker'], as_index=False)[['USD', 'BP']].sum()
    df_alpha_2020_group.columns = ['ticker', 'USD 2020', 'BP 2020']
    df_alpha_2021 = df_alpha['2021-01-01':'2021-12-31']
    df_alpha_2021_group = df_alpha_2021.groupby(['ticker'], as_index=False)[['USD', 'BP']].sum()
    df_alpha_2021_group.columns = ['ticker', 'USD 2021', 'BP 2021']
    df_alpha_2022 = df_alpha['2022-01-01':'2022-12-31']
    df_alpha_2022_group = df_alpha_2022.groupby(['ticker'], as_index=False)[['USD', 'BP']].sum()
    df_alpha_2022_group.columns = ['ticker', 'USD 2022', 'BP 2022']
    df_alpha_2023 = df_alpha['2023-01-01':'2023-12-31']
    df_alpha_2023_group = df_alpha_2023.groupby(['ticker'], as_index=False)[['USD', 'BP']].sum()
    df_alpha_2023_group.columns = ['ticker', 'USD 2023', 'BP 2023']
    df_alpha_2024 = df_alpha['2024-01-01':'2024-12-31']
    df_alpha_2024_group = df_alpha_2024.groupby(['ticker'], as_index=False)[['USD', 'BP']].sum()
    df_alpha_2024_group.columns = ['ticker', 'USD 2024', 'BP 2024']
    df_alpha_2025 = df_alpha['2025-01-01':end_date]
    df_alpha_2025_group = df_alpha_2025.groupby(['ticker'], as_index=False)[['USD', 'BP']].sum()
    df_alpha_2025_group.columns = ['ticker', 'USD 2025', 'BP 2025']

    df_alpha_merge = df_alpha_merge.merge(df_alpha_2019_group, on='ticker', how='left')
    df_alpha_merge = df_alpha_merge.merge(df_alpha_2020_group, on='ticker', how='left')
    df_alpha_merge = df_alpha_merge.merge(df_alpha_2021_group, on='ticker', how='left')
    df_alpha_merge = df_alpha_merge.merge(df_alpha_2022_group, on='ticker', how='left')
    df_alpha_merge = df_alpha_merge.merge(df_alpha_2023_group, on='ticker', how='left')
    df_alpha_merge = df_alpha_merge.merge(df_alpha_2024_group, on='ticker', how='left')
    df_alpha_merge = df_alpha_merge.merge(df_alpha_2025_group, on='ticker', how='left')

    df_alpha_merge = df_alpha_merge.fillna(0)
    df_alpha_merge = df_alpha_merge.sort_values(by='BP', ascending=True).reset_index(drop=True)

    # keep only bp columns
    df_alpha_merge = df_alpha_merge[['ticker', 'BP', 'BP 2019', 'BP 2020', 'BP 2021', 'BP 2022', 'BP 2023', 'BP 2024', 'BP 2025']]


    my_sql = """SELECT T2.ticker,CASE WHEN T1.mkt_value_usd > 0 THEN 'Long' ELSE 'Short' END AS position_type,
    COUNT(*) AS num_positions FROM position T1 JOIN product T2 ON T1.product_id = T2.id WHERE 
    T1.parent_fund_id = 1 AND T1.entry_date > '2019-04-01' AND T2.prod_type = 'Cash' GROUP BY T2.ticker, position_type
    ORDER BY T2.ticker, position_type;"""

    df_counts = pd.read_sql(my_sql, con=engine)
    df_counts_pivot = df_counts.pivot(index='ticker', columns='position_type', values='num_positions').fillna(0).reset_index()
    # put index as ticker column
    df_counts_pivot.columns.name = None
    df_alpha_merge = df_alpha_merge.merge(df_counts_pivot, on='ticker', how='left')
    df_alpha_merge = df_alpha_merge.fillna(0)
    # remove row when position_type is 0 for short
    df_alpha_merge = df_alpha_merge[df_alpha_merge['Short'] > 0]

    return df_alpha_merge


if __name__ == "__main__":
    start_date = '2019-04-01'
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    df_alpha = get_df_alpha(start_date, end_date)
    # export into with pandas
    df_alpha.to_excel('alpha_per_year.xlsx', index=False)
