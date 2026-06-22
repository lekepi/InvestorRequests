
from datetime import date, timedelta
from models import engine
import pandas as pd
import numpy as np
from utils import find_previous_date


def get_atom_perf():
    my_date = date(2025, 10, 7)

    my_sql = f"""SELECT product_id,ticker,prod_type,currency_id,is_cent FROM man_trade T1 JOIN product T2 on T1.product_id=T2.id 
    Group by product_id,ticker,prod_type;"""
    df_product = pd.read_sql(my_sql, con=engine)

    my_sql = f"""SELECT ticker,prod_type,product_id,notional,price,exec_qty,fx_rate,submitted_time,
    CAST(submitted_time AS DATE) AS trade_date FROM man_trade T1 JOIN product T2 ON T1.product_id = T2.id;"""
    df_trade = pd.read_sql(my_sql, con=engine)

    df_trade['atom_quantity'] = df_trade['exec_qty'] * 3
    # if ticker='SXO1 EUX' if atom_quantity>0, round down next integer, if atom_quantity<0, round up next integer
    mask = df_trade['ticker'] == 'SXO1 EUX'

    df_trade.loc[mask, 'atom_quantity'] = np.where(
        df_trade.loc[mask, 'atom_quantity'] > 0,
        np.floor(df_trade.loc[mask, 'atom_quantity']),
        np.ceil(df_trade.loc[mask, 'atom_quantity'])
    )

    df_product['position'] = 0
    trade_fee = 0

    df_result = pd.DataFrame(columns=[
        'entry_date',
        'long_usd',
        'short_usd',
        'net_usd',
        'gross_usd',
        'pnl_long_usd',
        'pnl_short_usd',
        'trading_usd',
        'pnl_position_usd',
        'pnl_trading_usd',
        'exec_fee_usd',
        'long_count',
        'short_count',
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
            atom_quantity = row['atom_quantity']
            exec_price = row['price']
            product_id = row['product_id']
            fx_rate = row['fx_rate']
            try:
                close_price = df_product[df_product['product_id'] == product_id]['close_price'].values[0]
            except Exception as e:
                print(f"Error for product_id={product_id}: {e}")
            is_cent = df_product[df_product['product_id'] == product_id]['is_cent'].values[0]
            prod_type = df_product[df_product['product_id'] == product_id]['prod_type'].values[0]

            pnl_trading_usd = atom_quantity * (close_price - exec_price) * np.where(prod_type == 'future', 50, 1) * fx_rate
            trading_usd = abs(atom_quantity) * exec_price * np.where(prod_type == 'future', 50, 1) * fx_rate
            exec_fee_usd = -(trading_usd) * trade_fee / 10000
            # add to df_product
            df_product.loc[df_product['product_id'] == product_id, 'position'] += atom_quantity
            df_product.loc[df_product['product_id'] == product_id, 'pnl_trading_usd'] = pnl_trading_usd
            df_product.loc[df_product['product_id'] == product_id, 'trading_usd'] = trading_usd
            df_product.loc[df_product['product_id'] == product_id, 'exec_fee_usd'] = exec_fee_usd

        # fill df_product with 0 when nan or none
        df_product = df_product.fillna(0)

        long_count = df_product.loc[df_product['notional_usd'] > 0, 'notional_usd'].count()
        short_count = df_product.loc[df_product['notional_usd'] < 0, 'notional_usd'].count()

        long_usd = df_product.loc[df_product['notional_usd'] > 0, 'notional_usd'].sum()
        short_usd = df_product.loc[df_product['notional_usd'] < 0, 'notional_usd'].sum()
        net_usd = df_product['notional_usd'].sum()
        gross_usd = df_product['notional_usd'].abs().sum()
        pnl_long_usd = df_product.loc[df_product['notional_usd'] > 0, 'pnl_position_usd'].sum()
        pnl_short_usd = df_product.loc[df_product['notional_usd'] < 0, 'pnl_position_usd'].sum()

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
            'pnl_long_usd': pnl_long_usd,
            'pnl_short_usd': pnl_short_usd,
            'trading_usd': trading_usd,
            'pnl_position_usd': pnl_position_usd,
            'pnl_trading_usd': pnl_trading_usd,
            'exec_fee_usd': exec_fee_usd,
            'long_count': long_count,
            'short_count': short_count
        }
        my_date += timedelta(days=1)

        # skip weekends (Saturday = 5, Sunday = 6)
        while my_date.weekday() >= 5:
            my_date += timedelta(days=1)

    df_result['pnl_usd'] = df_result['pnl_position_usd'] + df_result['pnl_trading_usd'] + df_result['exec_fee_usd']

    # remove first line
    df_result = df_result.iloc[1:]
    df_result = df_result.drop(columns=['trading_usd', 'pnl_position_usd', 'pnl_trading_usd', 'exec_fee_usd'])
    # store in excel
    df_result.to_excel('atom_daily_perf.xlsx', index=False)


def get_monthly_perf():
    aum = 25_000_000

    df = pd.read_excel('atom_daily_perf.xlsx')

    df['entry_date'] = pd.to_datetime(df['entry_date'])
    df['year_month'] = df['entry_date'].dt.to_period('M')

    df_monthly = (
        df.sort_values('entry_date')
          .groupby('year_month')
          .agg(
              long_usd=('long_usd', 'mean'),
              short_usd=('short_usd', 'mean'),
              gross_usd=('gross_usd', 'mean'),
              net_usd=('net_usd', 'mean'),

              long_count=('long_count', 'mean'),
              short_count=('short_count', 'mean'),

              pnl_long_usd=('pnl_long_usd', 'sum'),
              pnl_short_usd=('pnl_short_usd', 'sum'),
              pnl_usd=('pnl_usd', 'sum')
          )
          .assign(
              total_count=lambda x: x['long_count'] + x['short_count']
          )
          .reset_index()
    )

    # Scale ONLY USD columns to % of AUM
    usd_cols = [
        'long_usd',
        'short_usd',
        'gross_usd',
        'net_usd',
        'pnl_long_usd',
        'pnl_short_usd',
        'pnl_usd'
    ]

    df_monthly[usd_cols] = df_monthly[usd_cols] / aum

    # Rename USD columns to %
    df_monthly = df_monthly.rename(columns={
        'long_usd': 'long_pct',
        'short_usd': 'short_pct',
        'gross_usd': 'gross_pct',
        'net_usd': 'net_pct',
        'pnl_long_usd': 'pnl_long_pct',
        'pnl_short_usd': 'pnl_short_pct',
        'pnl_usd': 'pnl_pct',
    })

    # Round count columns
    df_monthly['long_count'] = df_monthly['long_count'].round(0).astype(int)
    df_monthly['short_count'] = df_monthly['short_count'].round(0).astype(int)
    df_monthly['total_count'] = df_monthly['total_count'].round(0).astype(int)

    df_monthly.to_excel('atom_monthly_perf.xlsx', index=False)


if __name__ == '__main__':
    # get_atom_perf()
    get_monthly_perf()
