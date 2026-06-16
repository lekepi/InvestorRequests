from models import engine
import pandas as pd


def get_stock_universe():
    my_sql = f"""SELECT distinct(ticker),name,isin,sedol FROM analyst_universe T1 
    JOIN PRODUCT T2 on T1.product_id=T2.id WHERE end_date is NULL and priority=1 order by ticker;"""

    df = pd.read_sql(my_sql, con=engine)
    # export into excel
    df.to_excel('Stock Universe.xlsx', index=False)


if __name__ == '__main__':
    get_stock_universe()
