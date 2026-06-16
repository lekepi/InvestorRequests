import sys
from datetime import date, timedelta
from models import engine
import pandas as pd
import numpy as np
from utils import find_previous_date
import calendar


# TODO: First you need to update the trades and position from MAN and update
#  the database and reconcile to make sure everything adds up

def get_ms_perf(my_date):

    my_sql = f"""SELECT product_id,ticker,prod_type,currency_id,is_cent FROM man_trade T1 JOIN product T2 on T1.product_id=T2.id 
    Group by product_id,ticker,prod_type;"""
    df_product = pd.read_sql(my_sql, con=engine)

    my_sql = f"""SELECT ticker,prod_type,product_id,notional,price,exec_qty,fx_rate,submitted_time,
    CAST(submitted_time AS DATE) AS trade_date FROM man_trade T1 JOIN product T2 ON T1.product_id = T2.id;"""
    df_trade = pd.read_sql(my_sql, con=engine)

    df_trade['ms_quantity'] = df_trade['exec_qty'] * 3
    # if ticker='SXO1 EUX' if ms_quantity>0, round down next integer, if ms_quantity<0, round up next integer
    mask = df_trade['ticker'] == 'SXO1 EUX'

    df_trade.loc[mask, 'ms_quantity'] = np.where(
        df_trade.loc[mask, 'ms_quantity'] > 0,
        np.floor(df_trade.loc[mask, 'ms_quantity']),
        np.ceil(df_trade.loc[mask, 'ms_quantity'])
    )

    df_product['position'] = 0
    trade_fee = 0

    df_result = pd.DataFrame(columns=[
        'entry_date',
        'long_usd',
        'short_usd',
        'net_usd',
        'gross_usd',
        'trading_usd',
        'pnl_position_usd',
        'pnl_trading_usd',
        'exec_fee_usd'
    ])

    while my_date <= date.today():
        df_product = df_product.drop(columns=['fx_rate', 'previous_price', 'close_price', 'return_1d', 'usd_price',
                                              'notional_usd', 'pnl_position_usd', 'trading_usd', 'pnl_trading_usd', 'exec_fee_usd'], errors='ignore')

        previous_date = find_previous_date(my_date)
        # get fx
        my_sql = f"""SELECT currency_id,rate as fx_rate FROM currency_history where entry_date='{previous_date}'"""
        df_fx = pd.read_sql(my_sql, con=engine)
        df_product = df_product.merge(
            df_fx[['currency_id', 'fx_rate']],
            on='currency_id',
            how='left'
        )

        my_sql = f"""SELECT product_id,price as previous_price FROM product_market_data WHERE entry_date='{previous_date}'"""
        df_previous_price = pd.read_sql(my_sql, con=engine)
        df_product = df_product.merge(
            df_previous_price[['product_id', 'previous_price']],
            on='product_id',
            how='left'
        )

        my_sql = f"""SELECT product_id,price as close_price FROM product_market_data WHERE entry_date='{my_date}'"""
        df_close_price = pd.read_sql(my_sql, con=engine)
        df_product = df_product.merge(
            df_close_price[['product_id', 'close_price']],
            on='product_id',
            how='left'
        )

        my_sql = f"""SELECT product_id,return_1d FROM product_beta WHERE entry_date='{my_date}'"""
        df_return_1d = pd.read_sql(my_sql, con=engine)
        df_product = df_product.merge(
            df_return_1d[['product_id', 'return_1d']],
            on='product_id',
            how='left'
        )
        # get notional of order (using previous closing price)
        df_product['notional_usd'] = (
                df_product['previous_price'] * df_product['position']
                * np.where(df_product['prod_type'] == 'future', 50, 1)
                * np.where(df_product['is_cent'], 1 / 100, 1)
                / df_product['fx_rate']
        )
        df_product['pnl_position_usd'] = df_product['notional_usd'] * df_product['return_1d']

        # now deal with trades: get new position and get pnl too
        # get trade for the day
        temp_trade = df_trade[df_trade['trade_date'] == my_date]
        for index, row in temp_trade.iterrows():
            ms_quantity = row['ms_quantity']
            exec_price = row['price']
            product_id = row['product_id']
            fx_rate = row['fx_rate']
            try:
                close_price = df_product[df_product['product_id'] == product_id]['close_price'].values[0]
            except Exception as e:
                print(f"Error for product_id={product_id}: {e}")
            is_cent = df_product[df_product['product_id'] == product_id]['is_cent'].values[0]
            prod_type = df_product[df_product['product_id'] == product_id]['prod_type'].values[0]

            pnl_trading_usd = ms_quantity * (close_price - exec_price) * np.where(prod_type == 'future', 50, 1) * fx_rate
            trading_usd = abs(ms_quantity) * exec_price * np.where(prod_type == 'future', 50, 1) * fx_rate
            exec_fee_usd = -(trading_usd) * trade_fee / 10000
            # add to df_product
            df_product.loc[df_product['product_id'] == product_id, 'position'] += ms_quantity
            df_product.loc[df_product['product_id'] == product_id, 'pnl_trading_usd'] = pnl_trading_usd
            df_product.loc[df_product['product_id'] == product_id, 'trading_usd'] = trading_usd
            df_product.loc[df_product['product_id'] == product_id, 'exec_fee_usd'] = exec_fee_usd

        # fill df_product with 0 when nan or none
        df_product = df_product.fillna(0)

        long_usd = df_product.loc[df_product['notional_usd'] > 0, 'notional_usd'].sum()
        short_usd = df_product.loc[df_product['notional_usd'] < 0, 'notional_usd'].sum()
        net_usd = df_product['notional_usd'].sum()
        gross_usd = df_product['notional_usd'].abs().sum()

        pnl_position_usd = df_product['pnl_position_usd'].sum()
        pnl_trading_usd = df_product.get('pnl_trading_usd', pd.Series(0)).sum()
        trading_usd = df_product.get('trading_usd', pd.Series(0)).sum()
        exec_fee_usd = df_product.get('exec_fee_usd', pd.Series(0)).sum()

        print(f"Date: {my_date} Done")

        df_result.loc[len(df_result)] = {
            'entry_date': my_date,
            'long_usd': long_usd,
            'short_usd': short_usd,
            'net_usd': net_usd,
            'gross_usd': gross_usd,
            'trading_usd': trading_usd,
            'pnl_position_usd': pnl_position_usd,
            'pnl_trading_usd': pnl_trading_usd,
            'exec_fee_usd': exec_fee_usd
        }
        my_date += timedelta(days=1)

        # skip weekends (Saturday = 5, Sunday = 6)
        while my_date.weekday() >= 5:
            my_date += timedelta(days=1)

    df_result['pnl_usd'] = df_result['pnl_position_usd'] + df_result['pnl_trading_usd'] + df_result['exec_fee_usd']
    df_result['Return vs Gross'] = df_result['pnl_usd'] / df_result['gross_usd']

    # remove first line
    df_result = df_result.iloc[1:]
    df_result = df_result.drop(columns=['trading_usd', 'pnl_position_usd', 'pnl_trading_usd', 'exec_fee_usd'])

    # Create a Pandas Excel writer using XlsxWriter as the engine
    writer = pd.ExcelWriter('LS_perf_analysis.xlsx', engine='xlsxwriter')
    df_result.to_excel(writer, index=False, sheet_name='Performance')

    # Get the xlsxwriter workbook and worksheet objects
    workbook = writer.book
    worksheet = writer.sheets['Performance']

    # Define the formatting
    int_format = workbook.add_format({'num_format': '#,##0'})
    pct_format = workbook.add_format({'num_format': '0.00%'})

    # Apply formats to specific columns
    # Columns B to F (indices 1 through 5) -> Integer with thousand separator
    worksheet.set_column(1, 5, 15, int_format)

    # Column G (index 6) -> Percentage
    worksheet.set_column(6, 6, 15, pct_format)

    # Close the writer to save the file
    writer.close()


def get_portfolio(my_date, is_df=False):
    next_date = my_date + timedelta(days=1)

    my_sql = f"""SELECT ticker,prod_type,product_id,notional,price,exec_qty,submitted_time,
    is_cent,multiplier,currency_id FROM man_trade T1 JOIN product T2 ON T1.product_id = T2.id
    WHERE submitted_time<'{next_date}';"""
    df_trade = pd.read_sql(my_sql, con=engine)

    df_trade['is_cent'] = df_trade['is_cent'].fillna(0)
    df_trade['multiplier'] = df_trade['multiplier'].fillna(1)
    #TODO: FOR ANY REPORT, QUANTITY TIMES 3
    df_trade['quantity'] = df_trade['exec_qty'] * 3
    # if ticker='SXO1 EUX' if quantity>0, round down next integer, if quantity<0, round up next integer
    mask = df_trade['ticker'] == 'SXO1 EUX'

    df_trade.loc[mask, 'quantity'] = np.where(
        df_trade.loc[mask, 'quantity'] > 0,
        np.floor(df_trade.loc[mask, 'quantity']),
        np.ceil(df_trade.loc[mask, 'quantity']))

    df_position = df_trade.groupby(['ticker', 'is_cent', 'multiplier', 'currency_id'],
        as_index=False)['quantity'].sum()
    df_position = df_position[df_position['quantity'] != 0]
    df_position['multiplier'] = df_position['multiplier'].fillna(1)

    my_sql = f"""SELECT currency_id,rate as fx_rate FROM currency_history where entry_date='{my_date}'"""
    df_fx = pd.read_sql(my_sql, con=engine)

    ticker_list = df_position['ticker'].unique().tolist()
    ticker_list_sql = ','.join([f"'{ticker}'" for ticker in ticker_list])
    my_sql = f"""SELECT ticker,price FROM product_market_data T1 JOIN product T2 on T1.product_id=T2.id
                 WHERE entry_date='{my_date}' and ticker in ({ticker_list_sql})"""
    df_close_price = pd.read_sql(my_sql, con=engine)

    df_position = df_position.merge(df_fx, left_on='currency_id', right_on='currency_id', how='left')
    df_position = df_position.merge(df_close_price, left_on='ticker', right_on='ticker', how='left')
    df_position['cent_factor'] = df_position['is_cent'].apply(lambda x: 0.01 if x == 1 else 1.0)
    df_position['notional_usd'] = (df_position['multiplier'] * df_position['price'] * df_position['quantity'] *
                                          df_position['cent_factor']) / df_position['fx_rate']

    if is_df:
        return df_position
    df_position.to_excel(f'LS_portfolio_{my_date}.xlsx', index=False)


def last_weekdays_before_today(start_year=2000):
    today = date.today()
    result = []

    for year in range(start_year, today.year + 1):
        end_month = 12

        if year == today.year:
            end_month = today.month - 1

        for month in range(1, end_month + 1):
            last_day = calendar.monthrange(year, month)[1]
            d = date(year, month, last_day)

            while d.weekday() >= 5:
                d -= timedelta(days=1)

            result.append(d)

    return result


def get_eom_portfolio(my_year):
    last_weekdays = last_weekdays_before_today()
    eom_dates = [d for d in last_weekdays if d.year == my_year]

    dfs = []
    for my_date in eom_dates:
        df_temp = get_portfolio(my_date, True)
        df_temp = df_temp.copy()
        df_temp["eom_date"] = my_date
        dfs.append(df_temp)
    df_eom_portfolio = pd.concat(dfs, ignore_index=True)
    # keep only the columns: eom_date, ticker, notional_usd
    df_eom_portfolio = df_eom_portfolio[['eom_date', 'ticker', 'notional_usd']]
    df_eom_portfolio = df_eom_portfolio.sort_values(by=['eom_date', 'notional_usd', 'ticker'],
                                                    ascending=[True, False, True])
    df_eom_portfolio.to_excel(f'LS_EOM_portfolio_{my_year}.xlsx', index=False)
    # order by eom_date, notional_usd desc, ticker


if __name__ == '__main__':
    # get EOM Portfolio
    get_eom_portfolio(2026)
    sys.exit()
    # my_date = date(2025, 10, 7)
    # get_ms_perf(my_date)

    my_date = date(2026, 5, 29)
    get_portfolio(my_date)
