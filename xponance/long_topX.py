from utils import last_alpha_date
import pandas as pd
from models import engine
from datetime import date


def get_top_position(position_count, start_date):

    # get month end date (to have the size of each position)
    my_sql = f"""SELECT YEAR(entry_date) AS year, MONTH(entry_date) AS month, MIN(entry_date) AS start_date FROM position 
    WHERE entry_date>='{start_date}' GROUP BY YEAR(entry_date),MONTH(entry_date) ORDER BY  year,month;"""
    df_date = pd.read_sql(my_sql, con=engine, parse_dates=['start_date'])
    # remove last row
    df_date = df_date[:-1]
    df_date['year_month'] = df_date['start_date'].dt.strftime('%Y-%m')
    date_list = df_date['start_date'].tolist()

    sql_string = ", ".join(f"'{date}'" for date in date_list)



    # top_position
    my_sql = f"""SELECT entry_date,T2.ticker,mkt_value_usd,pnl_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id 
    WHERE T1.parent_fund_id=1 and entry_date in ({sql_string}) and quantity>0 and prod_type='Cash' order by entry_date,mkt_value_usd desc;"""
    df_top_pos = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    df_top_pos['year_month'] = df_top_pos['entry_date'].dt.strftime('%Y-%m')

    df_result = pd.DataFrame(columns=['year_month', 'count', 'ticker', 'size'])

    for my_date in date_list:
        current_year_month = my_date.strftime('%Y-%m')
        df_top_pos_date = df_top_pos[df_top_pos['entry_date'] == my_date]
        df_top_pos_date = df_top_pos_date.head(position_count)
        total_notional = df_top_pos_date['mkt_value_usd'].sum()

        ticker_list = df_top_pos_date['ticker'].tolist()
        count = 1
        for ticker in ticker_list:
            size = df_top_pos_date[df_top_pos_date['ticker'] == ticker]['mkt_value_usd'].values[0] / total_notional
            df_result = df_result._append({'year_month': current_year_month, 'count': count, 'ticker': ticker,
                                           'size': size}, ignore_index=True)
            count += 1
    # export into excel
    df_result.to_excel(f'long_top{position_count}.xlsx', index=False)


if __name__ == '__main__':
    position_count = 20
    start_date = date(2019, 4, 1)
    get_top_position(position_count, start_date)
