from datetime import date
from models import engine
import pandas as pd
# find the % of Index in the short


if __name__ == '__main__':

    start_date = date(2019, 8, 1)
    end_date = date(2020, 10, 20)

    my_sql = f"""SELECT entry_date,T2.ticker,mkt_value_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id
        WHERE parent_fund_id=4 and entry_date>='{start_date}' and entry_date<='{end_date}' and prod_type='Cash' order by entry_date desc;"""

    df = pd.read_sql(my_sql, con=engine)

    df_spy = df[df['ticker'] == 'SPY US'].groupby('entry_date')['mkt_value_usd'].sum().rename('SPY')

    # Filter for short positions (negative market value) and group by date
    df_short = df[df['mkt_value_usd'] < 0].groupby('entry_date')['mkt_value_usd'].sum().rename('short')

    # Combine both into a single DataFrame
    df_result = pd.concat([df_spy, df_short], axis=1)

    # Optional: fill NaNs with 0 if desired
    df_result = df_result.fillna(0)

    # Set index explicitly (optional, for clarity)
    df_result.index.name = 'entry_date'
    df_result['pct'] = df_result['SPY'] / df_result['short']
    df_result = df_result.sort_index(ascending=False)
    df_result.to_excel('index_analysis.xlsx', index=True)