from models import engine
import pandas as pd


def net_inflows():
    my_sql = """SELECT year(entry_date),round(sum(additions-redemptions)/fx_rate/1000000,1) as net_inflows FROM investor_capital group by year(entry_date);"""
    df_inflows = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    print(df_inflows)


if __name__ == '__main__':

    net_inflows()