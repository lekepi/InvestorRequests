from models import engine
import pandas as pd


def display_aum():
    my_sql = """SELECT entry_date,amount FROM aum WHERE fund_id=4 and type !='leveraged' and month(entry_date)=12 order by entry_date;"""
    df_aum = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    df_aum['amount'] = df_aum['amount'] / 1000000
    df_aum['amount'] = df_aum['amount'].round(1)

    print(df_aum)


if __name__ == '__main__':

    display_aum()
