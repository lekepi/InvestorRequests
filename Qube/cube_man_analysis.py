from datetime import date, timedelta
from models import engine
import pandas as pd
import numpy as np
from utils import find_previous_date

# get_qube_trade() get only the trades for QUBE with their format (sizex3)
# get_qube_analysis() get the analysis for QUBE trades, including the PnL intraday, position exxec fee per day


def get_qube_trade():

    my_sql = f"""SELECT ticker,prod_type,product_id,notional,price,exec_qty,fx_rate,submitted_time,
        CAST(submitted_time AS DATE) AS trade_date FROM man_trade T1 JOIN product T2 ON T1.product_id = T2.id;"""
    df_trade = pd.read_sql(my_sql, con=engine)

    # if ticker='SXO1 EUX' replace with 'SXO1 Index' else add ' Equity at the end
    df_trade['ticker'] = np.where(df_trade['ticker'] == 'SXO1 EUX', 'SXO1 Index', df_trade['ticker'] + ' Equity')

    df_trade['submitted_time'] = pd.to_datetime(df_trade['submitted_time'])

    # Localize to London time (handles DST automatically)
    df_trade['submitted_time_london'] = df_trade['submitted_time'].dt.tz_localize('Europe/London')

    # Convert to US Eastern time (handles different DST rules automatically)
    df_trade['submitted_time_ny'] = df_trade['submitted_time_london'].dt.tz_convert('US/Eastern')

    df_trade['qube_qty'] = df_trade['exec_qty'] * 3
    mask = df_trade['ticker'] == 'SXO1 Index'

    df_trade.loc[mask, 'qube_qty'] = np.where(
        df_trade.loc[mask, 'qube_qty'] > 0,
        np.floor(df_trade.loc[mask, 'qube_qty']),
        np.ceil(df_trade.loc[mask, 'qube_qty'])
    )

    df_trade['submitted_time_london'] = df_trade['submitted_time_london'].dt.tz_localize(None)
    df_trade['submitted_time_ny'] = df_trade['submitted_time_ny'].dt.tz_localize(None)
    df_trade.to_excel('qube_trade.xlsx', index=False)

    df_out = df_trade[['submitted_time_ny', 'ticker', 'qube_qty']].copy()
    df_out.columns = ['TS - US Eastern time', 'ID_BBG', 'TRADED QUANTITY']

    df_out['TS - US Eastern time'] = df_out['TS - US Eastern time'].dt.tz_localize(None)

    with pd.ExcelWriter('Qube format Ananda LS.xlsx', engine='xlsxwriter') as writer:
        df_out.to_excel(writer, index=False, sheet_name='trades')

        workbook = writer.book
        worksheet = writer.sheets['trades']

        # 1. Define formats
        date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm:ss'})
        grey_format = workbook.add_format({'bg_color': '#F2F2F2'})  # Light grey

        # 2. Make columns larger (adjust the numbers '22', '20' as needed)
        worksheet.set_column('A:A', 22, date_format) # TS - US Eastern time column
        worksheet.set_column('B:B', 20)              # ID_BBG column
        worksheet.set_column('C:C', 20)              # TRADED QUANTITY column

        # 3. Apply alternating row colors
        # Get dataframe dimensions to know exactly where to apply the formatting
        max_row = len(df_out)
        max_col = len(df_out.columns) - 1

        # Apply conditional formatting from row 1 (skipping header) to max_row
        worksheet.conditional_format(1, 0, max_row, max_col, {
            'type': 'formula',
            'criteria': '=MOD(ROW(), 2) = 0',
            'format': grey_format
        })



def get_qube_analysis():
    my_date = date(2025, 10, 7)

    my_sql = f"""SELECT product_id,ticker,prod_type,currency_id,is_cent FROM man_trade T1 JOIN product T2 on T1.product_id=T2.id 
    Group by product_id,ticker,prod_type;"""
    df_product = pd.read_sql(my_sql, con=engine)

    my_sql = f"""SELECT ticker,prod_type,product_id,notional,price,exec_qty,fx_rate,submitted_time,
    CAST(submitted_time AS DATE) AS trade_date FROM man_trade T1 JOIN product T2 ON T1.product_id = T2.id;"""
    df_trade = pd.read_sql(my_sql, con=engine)

    df_trade['qube_qty'] = df_trade['exec_qty'] * 3
    # if ticker='SXO1 EUX' if qube_qty>0, round down next integer, if qube_qty<0, round up next integer
    mask = df_trade['ticker'] == 'SXO1 EUX'

    df_trade.loc[mask, 'qube_qty'] = np.where(
        df_trade.loc[mask, 'qube_qty'] > 0,
        np.floor(df_trade.loc[mask, 'qube_qty']),
        np.ceil(df_trade.loc[mask, 'qube_qty'])
    )

    df_product['position'] = 0
    trade_fee = 2

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
            qube_qty = row['qube_qty']
            exec_price = row['price']
            product_id = row['product_id']
            fx_rate = row['fx_rate']
            try:
                close_price = df_product[df_product['product_id'] == product_id]['close_price'].values[0]
            except Exception as e:
                print(f"Error for product_id={product_id}: {e}")
            is_cent = df_product[df_product['product_id'] == product_id]['is_cent'].values[0]
            prod_type = df_product[df_product['product_id'] == product_id]['prod_type'].values[0]

            pnl_trading_usd = qube_qty * (close_price - exec_price) * np.where(prod_type == 'future', 50, 1) * fx_rate
            trading_usd = abs(qube_qty) * exec_price * np.where(prod_type == 'future', 50, 1) * fx_rate
            exec_fee_usd = -(trading_usd) * trade_fee / 10000
            # add to df_product
            df_product.loc[df_product['product_id'] == product_id, 'position'] += qube_qty
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

    # export df_result to excel
    df_result.to_excel('qube_man_analysis.xlsx', index=False)
    df_trade.to_excel('qube_trade.xlsx', index=False)


if __name__ == '__main__':
    # get_qube_trade()
    get_qube_analysis()