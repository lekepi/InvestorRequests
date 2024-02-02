import pandas as pd
from models import engine


def get_exposure_attribution():

    # get list of start date of the month
    my_sql = """SELECT MIN(entry_date) AS first_date FROM position WHERE entry_date>='2019-04-01' 
    GROUP BY YEAR(entry_date), MONTH(entry_date) order by entry_date"""
    df_date = pd.read_sql(my_sql, con=engine, parse_dates=['first_date'])
    first_date_list = df_date['first_date'].tolist()
    first_date_str = ','.join([f"'{first_date}'" for first_date in first_date_list])

    # cash, ES1, SXO1

    my_sql = f"""SELECT T1.entry_date,T2.ticker,T2.prod_type,T4.name as country,T4.continent,T5.name as sector,sum(T1.mkt_value_usd) as notional_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id
LEFT JOIN exchange T3 on T2.exchange_id=T3.id LEFT JOIN country T4 on T3.country_id=T4.id
LEFT JOIN industry_sector T5 on T2.industry_sector_id=T5.id WHERE parent_fund_id=1 and entry_date in ({first_date_str}) and (T2.prod_type='Cash' or T2.ticker in ('ES1 CME', 'SXO1 EUX'))
GROUP BY entry_date,T2.ticker,T2.prod_type,country,continent,sector order by T2.ticker,entry_date;"""
    df_exposure = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    my_sql = """SELECT min(T1.entry_date) as entry_date,T2.ticker,T2.prod_type,T4.name as country,T4.continent,T5.name as sector,sum(T1.pnl_usd) as pnl_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id
LEFT JOIN exchange T3 on T2.exchange_id=T3.id LEFT JOIN country T4 on T3.country_id=T4.id
LEFT JOIN industry_sector T5 on T2.industry_sector_id=T5.id WHERE parent_fund_id=1 and entry_date>='2019-04-01' and (T2.prod_type='Cash' or T2.ticker in ('ES1 CME', 'SXO1 EUX'))
GROUP BY YEAR(entry_date),MONTH(entry_date),T2.ticker,T2.prod_type,country,continent,sector order by T2.ticker,entry_date;"""
    df_pnl = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    # market cap
    my_sql = f"""SELECT T1.entry_date,T2.ticker,CASE WHEN T3.market_cap<3000 then '0-3Bn' WHEN T3.market_cap<10000 THEN '3-10Bn' else '>10Bn' END as market_cap
FROM position T1 JOIN product T2 on T1.product_id=T2.id JOIN product_market_cap T3 on T1.product_id=T3.product_id and T1.entry_date=T3.entry_date and T3.type='Monthly'
WHERE T1.entry_date in ({first_date_str})  and T2.prod_type='Cash' and T1.parent_fund_id=1
GROUP by T1.entry_date,T2.ticker;"""
    df_market_cap = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    df_exposure = df_exposure.merge(df_market_cap, on=['entry_date', 'ticker'], how='left')
    df_exposure['market_cap'] = df_exposure.groupby('ticker')['market_cap'].fillna(method='ffill')
    df_exposure.sort_values(by='entry_date', ascending=False, inplace=True)

    df_pnl = df_pnl.merge(df_market_cap, on=['entry_date', 'ticker'], how='left')
    df_pnl['market_cap'] = df_pnl.groupby('ticker')['market_cap'].fillna(method='ffill')
    df_pnl.sort_values(by='entry_date', ascending=False, inplace=True)

    df = df_exposure.merge(df_pnl, on=['entry_date', 'ticker', 'prod_type', 'country', 'continent', 'sector', 'market_cap'], how='left')
    df['notional_usd'] = df['notional_usd'].fillna(0)
    df['pnl_usd'] = df['pnl_usd'].fillna(0)
    df['month_year'] = df['entry_date'].dt.strftime('%Y-%m')

    # df['sector'] = ' Index' when df['ticker'] in ('ES1 CME', 'SXO1 EUX') else df['sector']
    df['sector'] = df.apply(lambda x: 'Index' if x['ticker'] in ('ES1 CME', 'SXO1 EUX') else x['sector'], axis=1)

    # get aum
    my_sql = "Select entry_date,amount*1000000 as aum from aum where entry_date>='2019-04-01' " \
             "and type='leveraged' order by entry_date"
    df_aum = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    df_aum['month_year'] = df_aum['entry_date'].dt.strftime('%Y-%m')
    df_aum.set_index('month_year', inplace=True)
    # remove entry_date
    df_aum.drop(columns=['entry_date'], inplace=True)


    # All: general exposure and pnl
    df_all = df_aum
    # get exposure and pnl
    filtered_df = df[df['notional_usd'] > 0].groupby('month_year')[['notional_usd', 'pnl_usd']].sum()
    # rename column long_usd
    filtered_df.columns = ['long_expo_usd', 'long_pnl_usd']
    df_all = df_all.join(filtered_df, how='left')

    filtered_df = df[(df['notional_usd'] < 0) & (df['prod_type'] == 'Cash')].groupby('month_year')[['notional_usd', 'pnl_usd']].sum()
    # rename column single_short_usd
    filtered_df.columns = ['single_short_expo_usd', 'single_short_pnl_usd']
    df_all = df_all.join(filtered_df, how='left')

    filtered_df = df[(df['notional_usd'] < 0) & (df['prod_type'] != 'Cash')].groupby('month_year')[['notional_usd', 'pnl_usd']].sum()
    # rename column index_short_usd
    filtered_df.columns = ['index_short_expo_usd', 'index_short_pnl_usd']
    df_all = df_all.join(filtered_df, how='left')

    df_all['Long Expo'] = df_all['long_expo_usd'] * 2 / df_all['aum']
    df_all['Long PnL'] = df_all['long_pnl_usd'] * 2 / df_all['aum']
    df_all['Single Short Expo'] = df_all['single_short_expo_usd'] * 2 / df_all['aum']
    df_all['Single Short PnL'] = df_all['single_short_pnl_usd'] * 2 / df_all['aum']
    df_all['Index Short Expo'] = df_all['index_short_expo_usd'] * 2 / df_all['aum']
    df_all['Index Short PnL'] = df_all['index_short_pnl_usd'] * 2 / df_all['aum']
    df_all['Short Expo'] = df_all['Single Short Expo'] + df_all['Index Short Expo']
    df_all['Short PnL'] = df_all['Single Short PnL'] + df_all['Index Short PnL']
    df_all['Gross Expo'] = df_all['Long Expo'] - df_all['Short Expo']
    df_all['Net Expo'] = df_all['Long Expo'] + df_all['Short Expo']
    df_all['PnL'] = df_all['Long PnL'] + df_all['Short PnL']
    df_all = df_all.fillna(0)

    df_all_exposure = df_all[['Gross Expo', 'Net Expo', 'Long Expo', 'Short Expo', 'Single Short Expo', 'Index Short Expo']]
    df_all_pnl = df_all[['PnL', 'Long PnL', 'Short PnL', 'Single Short PnL', 'Index Short PnL']]

    # sort by month_year desc
    df_all_exposure = df_all_exposure.sort_index(ascending=False)
    df_all_pnl = df_all_pnl.sort_index(ascending=False)

    df_all_exposure.to_excel('Excel/exposure_all.xlsx')
    df_all_pnl.to_excel('Excel/pnl_all.xlsx')

    # Region: exposure and pnl by sector
    df_region = df_aum
    df_region_long = df[df['notional_usd'] > 0].groupby(['month_year', 'continent'])[['notional_usd', 'pnl_usd']].sum().unstack().fillna(0)
    # add 'Long' prefix to column names
    df_region_long.columns = [str(col) + ' - Long' for col in df_region_long.columns]
    df_region = df_region.join(df_region_long, how='left')

    df_region_short = df[(df['notional_usd'] < 0)].groupby(['month_year', 'continent'])[['notional_usd', 'pnl_usd']].sum().unstack().fillna(0)
    # add 'Single Short' prefix to column names
    df_region_short.columns = [str(col) + ' - Short' for col in df_region_short.columns]
    df_region = df_region.join(df_region_short, how='left')

    df_region_index = df[(df['notional_usd'] < 0) & (df['prod_type'] != 'Cash')].groupby(['month_year', 'continent'])[['notional_usd', 'pnl_usd']].sum().unstack().fillna(0)
    # add 'Index Short' prefix to column names
    df_region_index.columns = [str(col) + ' - Index Short' for col in df_region_index.columns]
    df_region = df_region.join(df_region_index, how='left')

    for col in df_region.columns[1:]:
        df_region[col] = df_region[col] * 2 / df_region['aum']
    # fill na
    df_region = df_region.fillna(0)
    # remove 'aum' column
    df_region = df_region.drop(columns=['aum'])

    df_region["('notional_usd', 'EMEA') - Stock Short"] = df_region["('notional_usd', 'EMEA') - Short"] - \
                                                         df_region["('notional_usd', 'EMEA') - Index Short"]

    df_region["('notional_usd', 'AMER') - Stock Short"] = df_region["('notional_usd', 'AMER') - Short"] - \
                                                        df_region["('notional_usd', 'AMER') - Index Short"]

    df_region["('pnl_usd', 'EMEA') - Stock Short"] = df_region["('pnl_usd', 'EMEA') - Short"] - \
                                                    df_region["('pnl_usd', 'EMEA') - Index Short"]

    df_region["('pnl_usd', 'AMER') - Stock Short"] = df_region["('pnl_usd', 'AMER') - Short"] - \
                                                    df_region["('pnl_usd', 'AMER') - Index Short"]

    df_region['EMEA - Net Expo'] = df_region["('notional_usd', 'EMEA') - Long"] + df_region["('notional_usd', 'EMEA') - Short"]
    df_region['AMER - Net Expo'] = df_region["('notional_usd', 'AMER') - Long"] + df_region["('notional_usd', 'AMER') - Short"]
    df_region['APAC - Net Expo'] = df_region["('notional_usd', 'APAC') - Long"] + df_region["('notional_usd', 'APAC') - Short"]

    df_region['EMEA - Net PnL'] = df_region["('pnl_usd', 'EMEA') - Long"] + df_region["('pnl_usd', 'EMEA') - Short"]
    df_region['AMER - Net PnL'] = df_region["('pnl_usd', 'AMER') - Long"] + df_region["('pnl_usd', 'AMER') - Short"]
    df_region['APAC - Net PnL'] = df_region["('pnl_usd', 'APAC') - Long"] + df_region["('pnl_usd', 'APAC') - Short"]

    region_expo_col = ['EMEA - Net Expo',
                       'AMER - Net Expo',
                       'APAC - Net Expo',
                       "('notional_usd', 'EMEA') - Long",
                       "('notional_usd', 'AMER') - Long",
                       "('notional_usd', 'APAC') - Long",
                       "('notional_usd', 'EMEA') - Short",
                       "('notional_usd', 'EMEA') - Stock Short",
                       "('notional_usd', 'EMEA') - Index Short",
                       "('notional_usd', 'AMER') - Short",
                       "('notional_usd', 'AMER') - Stock Short",
                       "('notional_usd', 'AMER') - Index Short"]

    region_pnl_col = ['EMEA - Net PnL',
                      'AMER - Net PnL',
                      'APAC - Net PnL',
                      "('pnl_usd', 'EMEA') - Long",
                      "('pnl_usd', 'AMER') - Long",
                      "('pnl_usd', 'APAC') - Long",
                      "('pnl_usd', 'EMEA') - Short",
                      "('pnl_usd', 'EMEA') - Stock Short",
                      "('pnl_usd', 'EMEA') - Index Short",
                      "('pnl_usd', 'AMER') - Short",
                      "('pnl_usd', 'AMER') - Stock Short",
                      "('pnl_usd', 'AMER') - Index Short"]

    df_region_expo = df_region[region_expo_col]
    df_region_pnl = df_region[region_pnl_col]

    df_region_expo.columns = ['EMEA - Net Expo', 'AMER - Net Expo', 'APAC - Net Expo',
                             'EMEA - Long Expo', 'AMER - Long Expo', 'APAC - Long Expo', 'EMEA - Short Expo', 'EMEA - Stock Short Expo',
                             'EMEA - Index Short Expo', 'AMER - Short Expo', 'AMER - Stock Short Expo', 'AMER - Index Short Expo']

    df_region_pnl.columns = ['EMEA - Net PnL', 'AMER - Net PnL', 'APAC - Net PnL',
                             'EMEA - Long PnL', 'AMER - Long PnL', 'APAC - Long PnL', 'EMEA - Short PnL', 'EMEA - Stock Short PnL',
                             'EMEA - Index Short PnL', 'AMER - Short PnL', 'AMER - Stock Short PnL', 'AMER - Index Short PnL']

    # sort by month_year desc
    df_region_expo = df_region_expo.sort_values(by='month_year', ascending=False)
    df_region_pnl = df_region_pnl.sort_values(by='month_year', ascending=False)

    df_region_expo.to_excel('Excel/exposure_region.xlsx')
    df_region_pnl.to_excel('Excel/pnl_region.xlsx')

    # Sector: exposure and pnl by sector
    df_sector = df_aum
    df_long_expo = df[df['notional_usd'] > 0].groupby(['month_year', 'sector'])[['notional_usd', 'pnl_usd']].sum().unstack().fillna(0)
    # add 'Long' prefix to column names
    df_long_expo.columns = [str(col) + ' - Long' for col in df_long_expo.columns]
    df_sector = df_sector.join(df_long_expo, how='left')

    df_short_expo = df[(df['notional_usd'] < 0)].groupby(['month_year', 'sector'])[['notional_usd', 'pnl_usd']].sum().unstack().fillna(0)
    # add 'Single Short' prefix to column names
    df_short_expo.columns = [str(col) + ' - Short' for col in df_short_expo.columns]
    df_sector = df_sector.join(df_short_expo, how='left')

    for col in df_sector.columns[1:]:
        df_sector[col] = df_sector[col] * 2 / df_sector['aum']
    # fill na
    df_sector = df_sector.fillna(0)
    # remove 'aum' column
    df_sector = df_sector.drop(columns=['aum'])

    sector_list = df['sector'].unique()
    # remove 'Index' from sector_list
    sector_list = [x for x in sector_list if x != 'Index']
    # sort
    sector_list.sort()

    sector_expo_col = [sector + ' - Long Expo' for sector in sector_list] + [sector + ' - Short Expo' for sector in sector_list]
    sector_expo_col.append('Index - Short Expo')
    sector_pnl_col = [sector + ' - Long PnL' for sector in sector_list] + [sector + ' - Short PnL' for sector in sector_list]
    sector_pnl_col.append('Index - Short PnL')

    for sector in sector_list:
        if f"('notional_usd', '{sector}') - Long" in df_sector.columns:
            df_sector[sector + ' - Long Expo'] = df_sector[f"('notional_usd', '{sector}') - Long"]
        else:
            df_sector[sector + ' - Long Expo'] = 0

        if f"('pnl_usd', '{sector}') - Long" in df_sector.columns:
            df_sector[sector + ' - Long PnL'] = df_sector[f"('pnl_usd', '{sector}') - Long"]
        else:
            df_sector[sector + ' - Long PnL'] = 0
    for sector in sector_list:
        if f"('notional_usd', '{sector}') - Short" in df_sector.columns:
            df_sector[sector + ' - Short Expo'] = df_sector[f"('notional_usd', '{sector}') - Short"]
        else:
            df_sector[sector + ' - Short Expo'] = 0

        if f"('pnl_usd', '{sector}') - Short" in df_sector.columns:
            df_sector[sector + ' - Short PnL'] = df_sector[f"('pnl_usd', '{sector}') - Short"]
        else:
            df_sector[sector + ' - Short PnL'] = 0

    df_sector['Index - Short Expo'] = df_sector[f"('notional_usd', 'Index') - Short"]
    df_sector['Index - Short PnL'] = df_sector[f"('pnl_usd', 'Index') - Short"]

    df_sector_exposure = df_sector[sector_expo_col]
    df_sector_pnl = df_sector[sector_pnl_col]

    # sort by month_year desc
    df_sector_exposure = df_sector_exposure.sort_values(by='month_year', ascending=False)
    df_sector_pnl = df_sector_pnl.sort_values(by='month_year', ascending=False)

    df_sector_exposure.to_excel('Excel/exposure_sector.xlsx')
    df_sector_pnl.to_excel('Excel/pnl_sector.xlsx')

    # marketCap: exposure and pnl by marketCap
    df_market_cap = df_aum
    # calculate df_long_expo for prod_type='Cash'
    df_long_expo = df[(df['notional_usd'] > 0) & (df['prod_type'] == 'Cash')].groupby(['month_year', 'market_cap'])[['notional_usd', 'pnl_usd']].sum().unstack().fillna(0)
    # add 'Long' prefix to column names
    df_long_expo.columns = [str(col) + ' - Long' for col in df_long_expo.columns]
    df_market_cap = df_market_cap.join(df_long_expo, how='left')

    df_short_expo = df[(df['notional_usd'] < 0) & (df['prod_type'] == 'Cash')].groupby(['month_year', 'market_cap'])[['notional_usd', 'pnl_usd']].sum().unstack().fillna(0)
    # add 'Single Short' prefix to column names
    df_short_expo.columns = [str(col) + ' - Short' for col in df_short_expo.columns]
    df_market_cap = df_market_cap.join(df_short_expo, how='left')

    for col in df_market_cap.columns[1:]:
        df_market_cap[col] = df_market_cap[col] * 2 / df_market_cap['aum']

    df_market_cap['Index - Short Expo'] = df_sector['Index - Short Expo']
    df_market_cap['Index - Short PnL'] = df_sector['Index - Short PnL']

    # fill na
    df_market_cap = df_market_cap.fillna(0)
    # remove 'aum' column
    df_market_cap = df_market_cap.drop(columns=['aum'])


    df_market_cap['0-3Bn - Net Expo'] = df_market_cap["('notional_usd', '0-3Bn') - Long"] + df_market_cap["('notional_usd', '0-3Bn') - Short"]
    df_market_cap['3-10Bn - Net Expo'] = df_market_cap["('notional_usd', '3-10Bn') - Long"] + df_market_cap["('notional_usd', '3-10Bn') - Short"]
    df_market_cap['>10Bn - Net Expo'] = df_market_cap["('notional_usd', '>10Bn') - Long"] + df_market_cap["('notional_usd', '>10Bn') - Short"]

    df_market_cap['0-3Bn - Net PnL'] = df_market_cap["('pnl_usd', '0-3Bn') - Long"] + df_market_cap["('pnl_usd', '0-3Bn') - Short"]
    df_market_cap['3-10Bn - Net PnL'] = df_market_cap["('pnl_usd', '3-10Bn') - Long"] + df_market_cap["('pnl_usd', '3-10Bn') - Short"]
    df_market_cap['>10Bn - Net PnL'] = df_market_cap["('pnl_usd', '>10Bn') - Long"] + df_market_cap["('pnl_usd', '>10Bn') - Short"]

    market_cap_expo_col = ['0-3Bn - Net Expo',
                           '3-10Bn - Net Expo',
                           '>10Bn - Net Expo',
                           "('notional_usd', '0-3Bn') - Long",
                           "('notional_usd', '3-10Bn') - Long",
                           "('notional_usd', '>10Bn') - Long",
                           "('notional_usd', '0-3Bn') - Short",
                           "('notional_usd', '3-10Bn') - Short",
                           "('notional_usd', '>10Bn') - Short",
                           'Index - Short Expo']

    market_cap_pnl_col = ['0-3Bn - Net PnL',
                          '3-10Bn - Net PnL',
                          '>10Bn - Net PnL',
                          "('pnl_usd', '0-3Bn') - Long",
                          "('pnl_usd', '3-10Bn') - Long",
                          "('pnl_usd', '>10Bn') - Long",
                          "('pnl_usd', '0-3Bn') - Short",
                          "('pnl_usd', '3-10Bn') - Short",
                          "('pnl_usd', '>10Bn') - Short",
                          'Index - Short PnL']

    df_market_cap_expo = df_market_cap[market_cap_expo_col]
    df_market_cap_pnl = df_market_cap[market_cap_pnl_col]

    df_market_cap_expo.columns = ['0-3Bn - Net Expo', '3-10Bn - Net Expo', '>10Bn - Net Expo',
                                  '0-3Bn - Long Expo', '3-10Bn - Long Expo', '>10Bn - Long Expo', '0-3Bn - Short Expo',
                                  '3-10Bn - Short Expo', '>10Bn - Short Expo', 'Index - Short Expo']
    df_market_cap_pnl.columns = ['0-3Bn - Net PnL', '3-10Bn - Net PnL','>10Bn - Net PnL',
                                 '0-3Bn - Long PnL', '3-10Bn - Long PnL', '>10Bn - Long PnL', '0-3Bn - Short PnL',
                                 '3-10Bn - Short PnL', '>10Bn - Short PnL', 'Index - Short PnL']

    df_market_cap_expo = df_market_cap_expo.sort_values(by='month_year', ascending=False)
    df_market_cap_pnl = df_market_cap_pnl.sort_values(by='month_year', ascending=False)

    df_market_cap_expo.to_excel('Excel/exposure_market_cap.xlsx')
    df_market_cap_pnl.to_excel('Excel/pnl_market_cap.xlsx')



if __name__ == '__main__':
    get_exposure_attribution()
