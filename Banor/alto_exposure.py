import pandas as pd
from models import engine
from datetime import date

###########################################
# TODO
# You need to format the file (dark color), freeze pane and add comment on the exposure calculation type
###########################################

def get_alto_exposure(my_date):

    my_sql = f"""SELECT entry_date,T2.ticker,T2.name as security_name,T2.isin,mkt_value_usd,prod_type FROM position T1 
        JOIN product T2 on T1.product_id=T2.id WHERE parent_fund_id=1 
        and entry_date='{my_date}' and (T2.prod_type='Cash' or T2.ticker in ('ES1 CME', 'SXO1 EUX'))
        order by mkt_value_usd desc;"""

    df_exposure = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    total_long = df_exposure[df_exposure['mkt_value_usd'] > 0]['mkt_value_usd'].sum()
    df_exposure['exposure_pct'] = df_exposure['mkt_value_usd'] / total_long

    df_exposure = df_exposure.drop(columns=['mkt_value_usd', 'prod_type'])
    # rename col
    df_exposure.columns = ['Date', 'Ticker', 'Sec. Name', 'Isin', 'Exposure']
    df_exposure.to_excel(f'Excel\\Alto Exposure - {my_date}.xlsx', index=False)

    print(1)


if __name__ == '__main__':
    my_date = date(2026, 4, 23)
    get_alto_exposure(my_date)
