from models import engine
import pandas as pd
from utils import decrypt_text
from config import ConfigDefault
from datetime import date, timedelta

secret_key = ConfigDefault.SECRET_KEY


def get_internal_investor():
    internal_investor_list = ['Caroline and Louis Villa', 'FO', 'Alexander and Araceli Strassburger',
                              'Ananda Asset Management Ltd', 'Dorothee Boissonnas']

    my_sql = """SELECT T3.encrypted_name,round(sum(ending_balance*ending_per/100/fx_rate),0) as balance_usd 
    FROM investor_capital T1 JOIN investor_alloc T2 on T2.investor_capital_id=T1.id JOIN investor T3 on T2.investor_id=T3.id
    WHERE T1.entry_date=(SELECT max(entry_date) FROM investor_capital) Group by investor_id order by balance_usd desc"""

    df = pd.read_sql(my_sql, con=engine)
    df['investor_name'] = df['encrypted_name'].apply(lambda x: decrypt_text(secret_key, x))

    aum = df['balance_usd'].sum()

    # filter
    df_internal = df[df['investor_name'].isin(internal_investor_list)]
    aum_internal = df_internal['balance_usd'].sum()

    internal_percent = aum_internal / aum
    return internal_percent


def get_average_trade(start_date):
    my_sql = f"SELECT trade_date,count(id) as trade_num FROM trade WHERE parent_fund_id=1 and trade_date>='{start_date}' group by trade_date;"
    df_trade = pd.read_sql(my_sql, con=engine)
    avg_daily_trade = df_trade['trade_num'].mean()
    return avg_daily_trade


def get_average_pos(start_date):
    my_sql = f"SELECT entry_date,count(id) as position_num FROM position WHERE entry_date>='{start_date}' and parent_fund_id=1 group by entry_date;"
    df_pos = pd.read_sql(my_sql, con=engine)
    avg_daily_pos = df_pos['position_num'].mean()
    return avg_daily_pos


def get_unencumbered(start_date):
    my_sql = f"""SELECT entry_date,account_value-margin_requirement as unencumbered,parent_broker_id FROM margin 
    WHERE entry_date>='{start_date}' order by entry_date;"""
    df_margin = pd.read_sql(my_sql, con=engine)
    df_pivot = df_margin.pivot_table(index='entry_date', columns='parent_broker_id', values='unencumbered', aggfunc='sum')
    df_pivot = df_pivot.dropna(axis=0, how='any')
    df_pivot['total'] = df_pivot.sum(axis=1)
    unencumbered_avg = df_pivot['total'].mean()
    return unencumbered_avg


if __name__ == '__main__':
    start_date = date(date.today().year, 1, 1)

    internal_percent = get_internal_investor()
    print(f"Internal AUM %: {internal_percent}")

    avg_daily_trade = get_average_trade(start_date)
    print(f"Average Daily Trade: {avg_daily_trade}")

    avg_daily_pos = get_average_pos(start_date)
    print(f"Average Daily Position: {avg_daily_pos}")

    unencumbered_avg = get_unencumbered(start_date)
    print(f"Average Unencumbered: {unencumbered_avg}")

