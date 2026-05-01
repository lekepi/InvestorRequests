import sys
from datetime import date, timedelta
from models import engine
import pandas as pd


def last_weekday_of_month(my_date: date) -> date:
    # Get the first day of the next month
    if my_date.month == 12:
        first_next_month = date(my_date.year + 1, 1, 1)
    else:
        first_next_month = date(my_date.year, my_date.month + 1, 1)

    # Step backward from the last day of this month
    last_day = first_next_month - timedelta(days=1)

    # While it's a weekend (Saturday=5, Sunday=6), step back
    while last_day.weekday() >= 5:
        last_day -= timedelta(days=1)

    return last_day


def first_weekday_of_next_month(my_date: date) -> date:
    # Get the first day of the next month
    if my_date.month == 12:
        first_day_next_month = date(my_date.year + 1, 1, 1)
    else:
        first_day_next_month = date(my_date.year, my_date.month + 1, 1)

    # While it's a weekend (Saturday=5, Sunday=6), step forward
    while first_day_next_month.weekday() >= 5:
        first_day_next_month += timedelta(days=1)

    return first_day_next_month


def previous_weekday(d: pd.Timestamp) -> pd.Timestamp:
    # If Monday, go back to Friday
    if d.weekday() == 0:
        return d - timedelta(days=3)
    # If Sunday, go back to Friday
    elif d.weekday() == 6:
        return d - timedelta(days=2)
    # Otherwise, just go back one day
    else:
        return d - timedelta(days=1)


def find_position_boothbay(my_date):
    my_sql = f"""SELECT entry_date,T2.ticker,'N/A' as ric,T2.sedol, T2.isin,'N/A' as Cusip,T2.name as identifier,bbg_type,T4.name as country,
    T5.name as sector,quantity,mkt_value_usd as notional1,mkt_value_usd as notional2,1 as delta,mkt_value_usd as notional3,'','USD' as cncy FROM position T1 JOIN product T2 on T1.product_id=T2.id JOIN exchange T3 
    on T2.exchange_id=T3.id JOIN country T4 on T3.country_id=T4.id JOIN industry_group T5 on T2.industry_group_id=T5.id
    JOIN Currency T6 on T2.currency_id=T6.id
    WHERE parent_fund_id=4 and entry_date='{my_date}' and prod_type='Cash' order by mkt_value_usd desc;"""
    df_position = pd.read_sql(my_sql, con=engine)

    df_position["entry_date"] = pd.to_datetime(df_position["entry_date"])  # ensure datetime
    df_position["entry_date"] = df_position["entry_date"].apply(previous_weekday)

    # change back entry_date to date
    df_position["entry_date"] = df_position["entry_date"].dt.date

    df_position['ticker'] = df_position['ticker'] + ' Equity'

    my_cols = ['Date', 'BBG', 'RIC', 'SEDOL', 'ISIN', 'CUSIP', 'Custom Identifier', 'Instrument Type', 'Listing Country',
               'Listing Sector', 'Shares/Qty', 'Notional Value', 'Net Market Value', 'Delta', 'Delta Adj Net Exp',
               'Portfolio Total AUM/NAV', 'Reporting Currency Code']
    df_position.columns = my_cols

    return df_position


def find_position_alto(my_date):
    my_sql = f"""SELECT entry_date,T2.ticker,'N/A' as ric,T2.sedol, T2.isin,'N/A' as Cusip,T2.name as identifier,bbg_type,T4.name as country,
    T5.name as sector,quantity,mkt_value_usd as notional1,mkt_value_usd as notional2,1 as delta,mkt_value_usd as notional3,'','USD' as cncy,prod_type FROM position T1 JOIN product T2 on T1.product_id=T2.id JOIN exchange T3 
    on T2.exchange_id=T3.id JOIN country T4 on T3.country_id=T4.id JOIN industry_group T5 on T2.industry_group_id=T5.id
    JOIN Currency T6 on T2.currency_id=T6.id
    WHERE parent_fund_id=1 and entry_date='{my_date}' and (prod_type='Cash' or T2.ticker in ('SXO1 EUX', 'ES1 CME')) order by mkt_value_usd desc;"""
    df_position = pd.read_sql(my_sql, con=engine)

    df_position["entry_date"] = pd.to_datetime(df_position["entry_date"])  # ensure datetime
    df_position["entry_date"] = df_position["entry_date"].apply(previous_weekday)

    # change back entry_date to date
    df_position["entry_date"] = df_position["entry_date"].dt.date

    # if prod_type='Cash' add Equity to ticker
    df_position.loc[df_position['prod_type'] == 'Cash', 'ticker'] = df_position['ticker'] + ' Equity'

    # replace 'SXO1 EUX' by 'SXO1 Index' and 'ES1 CME' by 'ES1 Index'
    df_position.loc[df_position['ticker'] == 'SXO1 EUX', 'ticker'] = 'SXO1 Index'
    df_position.loc[df_position['ticker'] == 'ES1 CME', 'ticker'] = 'ES1 Index'

    # if SXO1 Index' and 'ES1 CME' then set bbg_type to 'Index Future'
    df_position.loc[df_position['ticker'].isin(['SXO1 Index', 'ES1 Index']), 'bbg_type'] = 'Index Future'

    # if bbg_type is not 'Index Future' and notional1>=0 and notional1<300000 then remove the row
    df_position = df_position[~((df_position['bbg_type'] != 'Index Future') & (df_position['notional1'] >= 0) & (df_position['notional1'] < 300000))]

    #remove prod_type column
    df_position = df_position.drop(columns=['prod_type'])

    my_cols = ['Date', 'BBG', 'RIC', 'SEDOL', 'ISIN', 'CUSIP', 'Custom Identifier', 'Instrument Type', 'Listing Country',
               'Listing Sector', 'Shares/Qty', 'Notional Value', 'Net Market Value', 'Delta', 'Delta Adj Net Exp',
               'Portfolio Total AUM/NAV', 'Reporting Currency Code']
    df_position.columns = my_cols

    return df_position


if __name__ == '__main__':

    # ALTO
    start_date = date(2019, 4, 1)
    end_date = date(2025, 5, 20)

    my_date = start_date

    my_cols = ['Date', 'BBG', 'RIC', 'SEDOL', 'ISIN', 'CUSIP', 'Custom Identifier', 'Instrument Type', 'Listing Country',
               'Listing Sector', 'Shares/Qty', 'Notional Value', 'Net Market Value', 'Delta', 'Delta Adj Net Exp',
               'Portfolio Total AUM/NAV', 'Reporting Currency Code']
    df_position_final = pd.DataFrame(columns=my_cols)

    while my_date < end_date:
        my_date = first_weekday_of_next_month(my_date)
        df_position = find_position_alto(my_date)
        df_position_final = pd.concat([df_position_final, df_position], ignore_index=True)
        print(my_date)

    df_position_final.to_excel('EOM_positions ALTO.xlsx', index=False)
    sys.exit(0)


    # BOOTHBAY

    start_date = date(2019, 8, 1)
    end_date = date(2020, 10, 20)

    my_date = start_date

    my_cols = ['Date', 'BBG', 'RIC', 'SEDOL', 'ISIN', 'CUSIP', 'Custom Identifier', 'Instrument Type', 'Listing Country',
               'Listing Sector', 'Shares/Qty', 'Notional Value', 'Net Market Value', 'Delta', 'Delta Adj Net Exp',
               'Portfolio Total AUM/NAV', 'Reporting Currency Code']
    df_position_final = pd.DataFrame(columns=my_cols)

    while my_date < end_date:
        my_date = first_weekday_of_next_month(my_date)
        df_position = find_position_boothbay(my_date)
        df_position_final = pd.concat([df_position_final, df_position], ignore_index=True)
        print(my_date)

    df_position_final.to_excel('EOM_positions.xlsx', index=False)




