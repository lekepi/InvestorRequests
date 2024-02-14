import pandas as pd
from models import engine, config_class


"""
Asset_class: Get the % of asset class traded per year. 
We take the net volume traded by day by product (long+short in absolute value). 
We divide it by Cash Equity, CFD, Equity index and the rest
"""

def get_asset_class():
    my_sql = """SELECT T1.trade_date,T2.ticker,T2.prod_type,T4.name as country,T4.continent,
    T5.name as sector,T6.generic_future as security,abs(sum(T1.notional_usd)) as notional_usd 
    FROM trade T1 JOIN product T2 on T1.product_id=T2.id
    LEFT JOIN exchange T3 on T2.exchange_id=T3.id JOIN country T4 on T3.country_id=T4.id
    LEFT JOIN industry_sector T5 on T2.industry_sector_id=T5.id 
    LEFT JOIN security T6 on T2.security_id=T6.id WHERE parent_fund_id=1 and trade_date>='2019-04-01'
    and T2.ticker <>'EDZ2 CME'
    GROUP BY trade_date,T2.ticker,T2.prod_type,country,continent,sector order by T2.ticker,trade_date;"""
    df_trade = pd.read_sql(my_sql, con=engine, parse_dates=['trade_date'])

    # add column asset_class
    df_trade['asset_class'] = 'Other'

    # if prod_type is Cash, and country in ('united States, Canada, Switzerland'), then asset_class is 'Cash'
    df_trade.loc[(df_trade['prod_type'] == 'Cash') &
                 (df_trade['country'].isin(['United States', 'Canada', 'Switzerland'])), 'asset_class'] = 'Cash'
    # if prod_type is cash and country not in ('united States, Canada, Switzerland'), then asset_class is 'CFD'
    df_trade.loc[(df_trade['prod_type'] == 'Cash') &
                 (~df_trade['country'].isin(['United States', 'Canada', 'Switzerland'])), 'asset_class'] = 'CFD'
    # if ticker in ('SXO1 EUX', 'ES1 CME'), then asset_class is 'Equity Index'
    df_trade.loc[df_trade['security'].isin(['SXO1 EUX', 'ES1 CME']), 'asset_class'] = 'Equity Index'
    df_trade['notional_usd'] = df_trade['notional_usd'].fillna(0)
    # pivot table with asset_class as columns, year as index, and sum of notional_usd as values
    df_asset_class = pd.pivot_table(df_trade, values='notional_usd', index=df_trade['trade_date'].dt.year,
                                    columns='asset_class', aggfunc='sum', fill_value=0)

    df_asset_class = df_asset_class.div(df_asset_class.sum(axis=1), axis=0).round(4) * 100
    # into excel
    df_asset_class.to_excel('Excel\Asset Class per year.xlsx', index=True)


if __name__ == '__main__':
    get_asset_class()



