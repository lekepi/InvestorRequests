import pandas as pd
from models import engine, Product, session


if __name__ == '__main__':
    my_sql = """SELECT MAX(entry_date) AS last_date FROM position WHERE entry_date>='2019-04-01' GROUP BY YEAR(entry_date), MONTH(entry_date)"""
    df_date = pd.read_sql(my_sql, con=engine)
    last_date_list = df_date['last_date'].tolist()
    last_date_str = ','.join([f"'{last_date}'" for last_date in last_date_list])

    my_sql = f"""SELECT entry_date,T2.ticker,mkt_value_usd,abs(mkt_value_usd) as gross,prod_type 
    FROM position T1 JOIN product T2 on T1.product_id=T2.id and parent_fund_id=1 WHERE entry_date in ({last_date_str})
    and (prod_type='cash' or product_id in (439, 437))
    order by entry_date, mkt_value_usd, prod_type;"""
    df_pos = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    # convert entry_date into date
    df_pos['entry_date'] = pd.to_datetime(df_pos['entry_date']).dt.date

    df_result = pd.DataFrame(columns=['top10', 'bottom10', 'gross'], index=last_date_list)

    for my_date in last_date_list:
        df_pos_date = df_pos[df_pos['entry_date'] == my_date]
        total_notional = df_pos_date['gross'].sum()
        # keep only cash
        df_pos_date = df_pos_date[df_pos_date['prod_type'] == 'Cash']
        df_pos_date = df_pos_date.sort_values(by='mkt_value_usd', ascending=False)
        top10 = df_pos_date.head(10)['gross'].sum() / total_notional
        bottom10 = df_pos_date.tail(10)['gross'].sum() / total_notional

        df_result.loc[my_date, 'top10'] = top10
        df_result.loc[my_date, 'bottom10'] = bottom10
        df_result.loc[my_date, 'gross'] = total_notional

    # put result into excel
    df_result.to_excel('top_x_exposure.xlsx', index=True)





