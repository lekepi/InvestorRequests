import pandas as pd
from models import engine, session, Product
from datetime import date


def weekly_return(start_date, end_date, is_close_pnl):
    my_sql = f"""Select entry_date,amount*1000000 as aum from aum where entry_date>='2019-04-01' 
                    and entry_date>='{start_date}' and entry_date<='{end_date}' and type='leveraged' and fund_id=4 order by entry_date"""
    df_aum = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    my_sql = f"""SELECT T1.entry_date,T2.ticker,sum(T1.pnl_usd) as pnl_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id
    LEFT JOIN exchange T3 on T2.exchange_id=T3.id LEFT JOIN country T4 on T3.country_id=T4.id
    LEFT JOIN industry_group_gics T5 on T2.industry_group_gics_id=T5.id WHERE parent_fund_id=1
    and entry_date>='{start_date}' and entry_date<='{end_date}' and (T2.prod_type='Cash' or T2.ticker in ('ES1 CME', 'SXO1 EUX'))
    GROUP BY entry_date, T2.ticker order by entry_date;"""
    df_pnl = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    pnl_scalar = df_pnl['pnl_usd'].sum()

    if is_close_pnl:
        my_sql = f"""SELECT T1.trade_date as entry_date,
                CASE WHEN T3.generic_future IS NOT NULL THEN T3.generic_future ELSE T2.ticker END as tickerx,sum(T1.pnl_close) as pnl_close FROM trade T1 
                JOIN product T2 on T1.product_id=T2.id LEFT JOIN security T3 on T2.security_id=T3.id WHERE T1.parent_fund_id=1 and trade_date>='2019-04-01'
                and trade_date>='{start_date}' and trade_date<='{end_date}' and (T2.prod_type in ('Cash', 'Future') or T3.generic_future in 
                ('ES1 CME', 'SXO1 EUX')) group by entry_date,tickerx order by tickerx,T1.trade_date;"""

        df_pnl_close = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
        # rename  tickerx into ticker
        df_pnl_close = df_pnl_close.rename(columns={'tickerx': 'ticker'})

        pnl_close_scalar = df_pnl_close['pnl_close'].sum()

        # df_pnl = df_pnl.merge(df_pnl_close, on=['entry_date', 'ticker'], how='outer')

        df_merged_left = df_pnl.merge(df_pnl_close, on=['entry_date', 'ticker'], how='left')
        df_merged_right = df_pnl.merge(df_pnl_close, on=['entry_date', 'ticker'], how='right')
        df_pnl = pd.concat([df_merged_left, df_merged_right]).drop_duplicates()

        pnl_usd_scalar = df_pnl['pnl_usd'].sum()
        pnl_close_usd_scalar = df_pnl['pnl_close'].sum()

        df_pnl['pnl_close'] = df_pnl['pnl_close'].fillna(0)
        df_pnl['pnl_usd'] = df_pnl['pnl_usd'].fillna(0)
        df_pnl['pnl_usd'] = df_pnl['pnl_usd'] + df_pnl['pnl_close']
        # get scalar sum pnl_usd
        pnl = df_pnl['pnl_usd'].sum()

        # remove pnl_close
        df_pnl = df_pnl.drop(columns=['pnl_close'])
    df = df_pnl.copy()
    df['month_year'] = df['entry_date'].dt.strftime('%Y-%m')
    # put excel into df_pnl_remove
    df_pnl_remove = pd.read_excel(r'Pragma\Rec.xlsx', sheet_name='Ignore')
    # remove from df_pnl_close the rows that are in df_pnl_remove
    df = df[~df.set_index(['ticker', 'month_year']).index.isin(
        df_pnl_remove.set_index(['ticker', 'month_year']).index)]
    # group by entry_date, sum the perf
    df = df.groupby('entry_date').agg({'pnl_usd': 'sum'})
    df = df.merge(df_aum, on=['entry_date'], how='left')
    # fill aum with previous value
    df['aum'] = df['aum'].fillna(method='ffill')
    df['Perf'] = 2*df['pnl_usd'] / df['aum']

    # export in excel
    df.to_excel('Excel\weekly_return.xlsx')


if __name__ == '__main__':
    start_date = date(2019, 4, 1)
    end_date = date(2023, 12, 31)
    is_close_pnl = True
    weekly_return(start_date, end_date, is_close_pnl)
