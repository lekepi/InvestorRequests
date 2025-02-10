from utils import last_alpha_date
import pandas as pd
from models import engine, Product, session
from datetime import date


# Function to get the last weekday of the previous month
def last_weekday_of_previous_month(date):
    # Move to the start of the current month, subtract one day to get to the last day of the previous month
    last_day_prev_month = date.replace(day=1) - pd.Timedelta(days=1)
    # if sunday, minus 2 days
    if last_day_prev_month.weekday() == 6:
        last_day_prev_month = last_day_prev_month - pd.Timedelta(days=2)
    # if saturday, minus 1 day
    elif last_day_prev_month.weekday() == 5:
        last_day_prev_month = last_day_prev_month - pd.Timedelta(days=1)
    last_business_day = last_day_prev_month
    return last_business_day


def last_weekday_of_current_month(date):
    next_month_date = date.replace(day=28) + pd.Timedelta(days=4)
    last_day_current_month = next_month_date.replace(day=1) - pd.Timedelta(days=1)
    if last_day_current_month.weekday() == 6:
        last_day_current_month = last_day_current_month - pd.Timedelta(days=2)
    # if saturday, minus 1 day
    elif last_day_current_month.weekday() == 5:
        last_day_current_month = last_day_current_month - pd.Timedelta(days=1)
    last_business_day = last_day_current_month
    return last_business_day


def get_top_position(position_count, start_date, end_date):

    product_db = session.query(Product).filter(Product.prod_type == 'Cash').all()

    # get month end date (to have the size of each position)
    my_sql = f"""SELECT YEAR(entry_date) AS year, MONTH(entry_date) AS month, MIN(entry_date) AS start_date FROM position 
    WHERE entry_date>='{start_date}' and entry_date<'{end_date}' GROUP BY YEAR(entry_date),MONTH(entry_date) ORDER BY year,month;"""
    df_date = pd.read_sql(my_sql, con=engine, parse_dates=['start_date'])
    # remove last row
    df_date = df_date[:-1]
    date_list = df_date['start_date'].tolist()

    sql_string = ", ".join(f"'{date}'" for date in date_list)

    # top_position
    my_sql = f"""SELECT entry_date,T2.ticker,mkt_value_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id 
    WHERE T1.parent_fund_id=1 and entry_date in ({sql_string}) and quantity>0 and prod_type='Cash' order by entry_date,mkt_value_usd desc;"""
    df_top_pos = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    df_top_pos['year_month'] = df_top_pos['entry_date'].dt.strftime('%Y-%m')
    df_top_pos['year'] = df_top_pos['entry_date'].dt.strftime('%Y')

    df_result = pd.DataFrame(columns=['entry_date', 'year_month', 'year', 'count', 'ticker', 'sedol', 'name', 'size'])

    for my_date in date_list:
        current_year_month = my_date.strftime('%Y-%m')
        current_year = my_date.strftime('%Y')
        df_top_pos_date = df_top_pos[df_top_pos['entry_date'] == my_date]
        df_top_pos_date = df_top_pos_date.head(position_count)
        total_notional = df_top_pos_date['mkt_value_usd'].sum()

        ticker_list = df_top_pos_date['ticker'].tolist()
        count = 1
        for ticker in ticker_list:

            sedol = [product.sedol for product in product_db if product.ticker == ticker][0]
            name = [product.name for product in product_db if product.ticker == ticker][0]
            size = df_top_pos_date[df_top_pos_date['ticker'] == ticker]['mkt_value_usd'].values[0] / total_notional
            df_result = df_result._append({'entry_date': my_date, 'year_month': current_year_month,
                                           'year': current_year, 'count': count, 'ticker': ticker, 'sedol': sedol,
                                           'name': name,   'size': size}, ignore_index=True)
            count += 1

    df_result['start_date'] = df_result['entry_date'].apply(last_weekday_of_previous_month)
    df_result['end_date'] = df_result['entry_date'].apply(last_weekday_of_current_month)

    # convert start_date and end_date into date, not datetime
    df_result['start_date'] = df_result['start_date'].dt.date
    df_result['end_date'] = df_result['end_date'].dt.date

    ticker_list = df_result['ticker'].unique().tolist()
    date_price_list = df_result['start_date'].unique().tolist()
    # get max(end_date) from the top position
    end_date = df_result['end_date'].max()
    date_price_list.append(end_date)

    ticker_sql = ", ".join(f"'{ticker}'" for ticker in ticker_list)
    date_price_sql = ", ".join(f"'{date}'" for date in date_price_list)

    my_sql = f"""SELECT entry_date as start_date,ticker,adj_price FROM product_market_data T1 JOIN product T2 on T1.product_id=T2.id
    WHERE ticker in ({ticker_sql}) and entry_date in ({date_price_sql});"""
    df_price = pd.read_sql(my_sql, con=engine, parse_dates=['start_date'])
    # convert start_date into date, not datetime
    df_price['start_date'] = df_price['start_date'].dt.date

    df_result['start_price'] = None
    df_result['end_price'] = None

    for index, row in df_result.iterrows():
        start_price = df_price[(df_price['ticker'] == row['ticker']) & (df_price['start_date'] == row['start_date'])]['adj_price'].values[0]
        end_price = df_price[(df_price['ticker'] == row['ticker']) & (df_price['start_date'] == row['end_date'])]['adj_price'].values[0]
        df_result.at[index, 'start_price'] = start_price
        df_result.at[index, 'end_price'] = end_price

    df_result['return'] = (df_result['end_price'] - df_result['start_price']) / df_result['start_price']
    df_result['wgt. return'] = df_result['return'] * df_result['size']

    df_result.to_excel(f'long_top{position_count}.xlsx', index=False)


if __name__ == '__main__':

    position_count = 20
    start_date = date(2019, 4, 1)
    end_date = date(2025, 1, 5)
    # x = last_weekday_of_previous_month(start_date)
    get_top_position(position_count, start_date, end_date)
