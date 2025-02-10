
import pandas as pd
from models import engine, session, Investor
from utils import simple_email, encrypt_text, decrypt_text
from config import ConfigDefault
from datetime import datetime

secret_key = ConfigDefault.SECRET_KEY


def get_capital_movement(frequency='Year'):
    # get the additions/redemptions per month (taking into account the switch)

    print("Current Time:", datetime.now().time())
    my_sql = """SELECT T1.id,T2.investor_id,entry_date as report_date,T1.fund_type,T1.class_name,sum(additions*additions_per/100) as additions,
        sum(redemptions*redemptions_per/100) as redemptions,sum(ending_balance*ending_per/100) as ending_balance,T3.code as cncy,
        sum(additions*additions_per/100/fx_rate) as additions_usd, sum(redemptions*redemptions_per/100/fx_rate) as redemptions_usd, 
        sum(ending_balance*ending_per/100/fx_rate) as ending_balance_usd FROM investor_capital T1 
        JOIN investor_alloc T2 on T1.id=T2.investor_capital_id JOIN currency T3 on T1.cncy_id=T3.id
        group by entry_date,report_date,T1.fund_type,T1.class_name,T3.code,investor_id
        HAVING (abs(sum(additions*additions_per/100))>0.1 or abs(sum(redemptions*redemptions_per/100))>0.1)
        order by entry_date desc, fund_type asc,redemptions asc;"""

    # get all additions/redemtions
    df_all_change = pd.read_sql(my_sql, con=engine, index_col='id')

    investor_id_list = df_all_change['investor_id'].unique().tolist()
    report_date_list = df_all_change['report_date'].unique().tolist()
    # sort by date
    report_date_list.sort()
    my_date = report_date_list[-1]

    # create df with col = investor
    df_investor_amount = pd.DataFrame(index=report_date_list, columns=investor_id_list)
    df_investor_amount = df_investor_amount.fillna(0)

    for investor_id in investor_id_list:
        df_change = df_all_change[df_all_change['investor_id'] == investor_id].copy()
        df_change['addition_type'] = None
        df_change['redemption_type'] = None

        #df_change.loc[:, 'addition_type'] = None
        #df_change.loc[:, 'redemption_type'] = None

        df_change.loc[df_change['additions'] == 0, 'addition_type'] = "Empty"
        df_change.loc[df_change['redemptions'] == 0, 'redemption_type'] = "Empty"
        date_list = sorted(df_change['report_date'].unique())

        for i, curr_date in enumerate(date_list):
            is_loop = True
            while is_loop:
                df_change, is_loop = get_investor_change(df_change, curr_date, date_list, i)
                pass

        # df_change.loc[df_change['report_date'] == my_date, 'addition_type'] = "Switch"
        # df_change.loc[df_change['report_date'] == my_date, 'redemption_type'] = "Switch"

        # add df_change to df_investor
        for index, row in df_change.iterrows():
            addition_usd = row['additions_usd']
            redemption_usd = row['redemptions_usd']

            if (not row['addition_type'] or "Switch" not in row['addition_type']) and addition_usd > 0:
                df_investor_amount.loc[row['report_date'], row['investor_id']] += addition_usd

            if (not row['redemption_type'] or "Switch" not in row['redemption_type']) and redemption_usd > 0:
                df_investor_amount.loc[row['report_date'], row['investor_id']] -= redemption_usd
            pass
        pass

    investor_db = session.query(Investor).all()
    # replace investor_id with investor_name in df_investor_amount columns
    for investor in investor_db:
        investor_id = investor.id
        investor_name = decrypt_text(secret_key, investor.encrypted_name)
        df_investor_amount.rename(columns={investor_id: investor_name}, inplace=True)

    # add col Additions that is the sum of positive value
    df_investor_amount['Additions'] = df_investor_amount.apply(lambda x: x[x > 0].sum(), axis=1)
    # add col Redemptions that is the sum of negative value
    df_investor_amount['Redemptions'] = df_investor_amount.apply(lambda x: x[x < 0].sum(), axis=1)

    df_add_red = df_investor_amount[['Additions', 'Redemptions']]
    # convert index into date
    df_add_red.index = pd.to_datetime(df_add_red.index)
    if frequency == 'Year':
        df_add_red = df_add_red.groupby(df_add_red.index.year).sum()
    elif frequency == 'Month':
        # replace datetime index by -YYYY-MM'
        df_add_red.index = df_add_red.index.strftime('%Y-%m')
        df_add_red = df_add_red.groupby(df_add_red.index).sum()
        full_range = pd.date_range(start=df_add_red.index.min(), end=df_add_red.index.max(), freq='MS').strftime('%Y-%m')
        df_add_red = df_add_red.reindex(full_range, fill_value=0)

        df_add_red['Redemptions'] = df_add_red['Redemptions'].shift(1, fill_value=0)

        # reformat with thousand separator
    df_add_red['Additions'] = df_add_red['Additions'].apply(lambda x: "{:,.0f}".format(x))
    df_add_red['Redemptions'] = df_add_red['Redemptions'].apply(lambda x: "{:,.0f}".format(x))
    print("Current Time:", datetime.now().time())
    return df_add_red


def get_investor_change(df_change, curr_date, date_list, i):
    # get the number of new investors and leaving investors per year
    df_change_curr = df_change.loc[(df_change['report_date'] == curr_date)]
    df_change_next = pd.DataFrame()
    if i + 1 < len(date_list):
        next_date = date_list[i + 1]
        if (next_date - curr_date).days < 40:
            df_change_next = df_change.loc[df_change['report_date'] == next_date]

    # Per Line - same USD amount +- 5% same month
    for j, row1 in df_change_curr.iterrows():
        if not row1['addition_type']:
            for k, row2 in df_change_curr.iterrows():
                if j != k:
                    if not row2['addition_type']:
                        if abs(row1['additions_usd'] + row2['additions_usd']) < 10:
                            df_change.loc[j, 'addition_type'] = 'Switch'
                            df_change.loc[k, 'addition_type'] = 'Switch'
                            return df_change, True
                    if not row2['redemption_type']:
                        if abs(row2['redemptions_usd'] / row1['additions_usd'] - 1) < 0.05:
                            df_change.loc[j, 'addition_type'] = 'Switch'
                            df_change.loc[k, 'redemption_type'] = 'Switch'
                            return df_change, True
    # Per Line - same USD amount +- 5% next month
    if not df_change_next.empty:
        for j, row1 in df_change_curr.iterrows():
            if not row1['redemption_type']:
                for k, row2 in df_change_next.iterrows():
                    if j != k:
                        if not row2['addition_type']:
                            if abs(row1['redemptions_usd'] / row2['additions_usd'] - 1) < 0.05:
                                df_change.loc[k, 'addition_type'] = 'Switch'
                                df_change.loc[j, 'redemption_type'] = 'Switch'
                                return df_change, True
                                return df_change, True
    # Per Day - same USD amount +- 5% same month
    df_change_curr = df_change.loc[(df_change['report_date'] == curr_date)]
    cur_day_redemption = df_change_curr.loc[df_change_curr['redemption_type'].isnull(), 'redemptions_usd'].sum()
    cur_day_addition = df_change_curr.loc[df_change_curr['addition_type'].isnull(), 'additions_usd'].sum()
    if cur_day_addition > 0:
        if abs(cur_day_redemption / cur_day_addition - 1) < 0.1:
            for l, row in df_change_curr.iterrows():
                if row['redemption_type'] == None:
                    df_change.loc[l, 'redemption_type'] = 'Multi Switch'
                if row['addition_type'] == None:
                    df_change.loc[l, 'addition_type'] = 'Multi Switch'
            return df_change, True
        # Per day Additions > Redemptions, try first to find one addition > sum(redemptions)
        elif cur_day_addition > cur_day_redemption and cur_day_redemption>0:
            for l, row in df_change_curr.iterrows():
                if row['redemption_type'] == None:
                    df_change.loc[l, 'redemption_type'] = 'Extra Switch'
            for l, row in df_change_curr.iterrows():
                if row['addition_type'] == None and row['additions_usd'] > cur_day_redemption:
                    df_change.loc[l, 'addition_type'] = 'Partial'
                    return df_change, True
            # if one addition line not enough, mark all additions as partial
            for l, row in df_change_curr.iterrows():
                if row['addition_type'] == None:
                    df_change.loc[l, 'addition_type'] = 'Partial'
        # Per day Additions < Redemptions, try first to find one redemption > sum(additions)
        elif cur_day_addition < cur_day_redemption and cur_day_addition > 0:
            for l, row in df_change_curr.iterrows():
                if row['addition_type'] == None:
                    df_change.loc[l, 'addition_type'] = 'Extra Switch'
            for l, row in df_change_curr.iterrows():
                if row['redemption_type'] == None and row['redemptions_usd'] > cur_day_addition:
                    df_change.loc[l, 'redemption_type'] = 'Partial'
                    return df_change, True
            # if one redemption line not enough, mark all redemptions as partial
            for l, row in df_change_curr.iterrows():
                if row['redemption_type'] == None:
                    df_change.loc[l, 'redemption_type'] = 'Partial'

            return df_change, True
    return df_change, False


def get_investor_in_out():
    my_sql = """SELECT T1.id,T3.encrypted_name as investor,entry_date as report_date,
        sum(ending_balance*ending_per/100/fx_rate) as ending_balance_usd FROM investor_capital T1 
        JOIN investor_alloc T2 on T1.id=T2.investor_capital_id JOIN investor T3 on T2.investor_id=T3.id
        group by entry_date,report_date,investor_id order by entry_date"""
    df = pd.read_sql(my_sql, con=engine)
    # pivot table with report_date as index, investor as columns and ending_balance_usd as values (sum)
    df_pivot = df.pivot_table(index='report_date', columns='investor', values='ending_balance_usd', aggfunc='sum')
    # fill with 0
    df_pivot.fillna(0, inplace=True)

    df_sign = pd.DataFrame(0, index=df_pivot.index, columns=df_pivot.columns)

    for column in df_pivot.columns:
        if df_pivot[column][0] > 0:
            df_sign.at[df_pivot.index[0], column] = 1

        for i in range(1, len(df_pivot)):
            if df_pivot[column][i] > 0 and df_pivot[column][i - 1] == 0:
                df_sign.at[df_pivot.index[i], column] = 1
            elif df_pivot[column][i] == 0 and df_pivot[column][i - 1] > 0:
                df_sign.at[df_pivot.index[i], column] = -1

    df_sign['investor_in'] = df_sign[df_sign == 1].sum(axis=1)
    df_sign['investor_out'] = df_sign[df_sign == -1].sum(axis=1)

    df_inv_in_out = df_sign[['investor_in', 'investor_out']]
    # convert index into date
    df_inv_in_out.index = pd.to_datetime(df_inv_in_out.index)
    # group by year
    df_inv_in_out = df_inv_in_out.groupby(df_inv_in_out.index.year).sum()
    # reformat as integer
    df_inv_in_out = df_inv_in_out.astype(int).astype(str)

    return df_inv_in_out


if __name__ == '__main__':
    df_add_red = get_capital_movement(frequency='Month')  # Month, Year
    df_inv_in_out = get_investor_in_out()

    html = "<html><body><h3>Capital Change</h3>"
    html += df_add_red.to_html()
    html += "<br><h3>Investor Change</h3>"
    html += df_inv_in_out.to_html()

    simple_email("ADIA DDQ - Capital & asset change", '', 'olivier@ananda-am.com', html)

