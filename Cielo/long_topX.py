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

    # get aum (equivalent for leveraged fund /2)
    my_sql = f"""SELECT entry_date,amount*1000000/2*deployed/100 as aum,deployed FROM aum WHERE fund_id=4 and type='leveraged' and 
    entry_date>='{start_date}' order by entry_date;"""
    df_aum = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df_aum['year_month'] = df_aum.index.strftime('%Y-%m')
    date_list = df_date['start_date'].tolist()

    # top_position
    my_sql = f"""SELECT entry_date,T2.ticker,mkt_value_usd,pnl_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id 
    WHERE T1.parent_fund_id=1 and entry_date>='{start_date}' and quantity>0 and prod_type='Cash' order by entry_date,mkt_value_usd desc;"""
    df_top_pos = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    df_top_pos['year_month'] = df_top_pos['entry_date'].dt.strftime('%Y-%m')

    df_result = pd.DataFrame(columns=['year_month', 'count', 'ticker', 'size', 'return'])

    for my_date in date_list:
        current_year_month = my_date.strftime('%Y-%m')
        aum = df_aum[df_aum['year_month'] == current_year_month]['aum'].iloc[0]
        df_top_pos_date = df_top_pos[df_top_pos['entry_date'] == my_date]
        # get only  the top position_count
        df_top_pos_date = df_top_pos_date.head(position_count)
        ticker_list = df_top_pos_date['ticker'].tolist()
        count = 1
        for ticker in ticker_list:
            notional = df_top_pos_date[df_top_pos_date['ticker'] == ticker]['mkt_value_usd'].iloc[0]
            # get sum of pnl_usd in df_top_pos for that ticker and year_month
            pnl = df_top_pos[(df_top_pos['ticker'] == ticker) & (df_top_pos['year_month'] ==
                                                                 current_year_month)]['pnl_usd'].sum()
            size = notional / aum
            return_ = pnl / aum
            df_result = df_result._append({'year_month': current_year_month, 'count':count, 'ticker': ticker,
                                           'size': size, 'return': return_}, ignore_index=True)
            count += 1
    # export into excel
    df_result.to_excel(f'long_top{position_count}.xlsx', index=False)


if __name__ == '__main__':
    position_count = 5
    start_date = date(2020, 1, 1)
    get_top_position(position_count, start_date)
