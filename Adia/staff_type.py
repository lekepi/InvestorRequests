from models import engine, Employee
import pandas as pd
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta


def last_dates_of_quarters(start_date, end_date):

    # List to store the last dates of quarters
    quarter_dates = []

    # Start with the first quarter's end date
    current = start_date.replace(month=3, day=31)
    if start_date > current:
        # Move to the next quarter if start date is after the first quarter
        current = current + relativedelta(months=3)

    # Loop through quarters until the end date
    while current <= end_date:
        quarter_dates.append(current)
        current = current + relativedelta(months=3)

    return quarter_dates


if __name__ == '__main__':

    df_result = pd.DataFrame(columns=['quarter', 'investment_num', 'non_investment_num'])

    last_date_list = last_dates_of_quarters(date(2019, 3, 31), date.today())

    my_sql = "SELECT first_name,start_date,end_date,status from employee WHERE status<>'Intern';"
    df = pd.read_sql(my_sql, con=engine, parse_dates=['start_date', 'end_date'])

    #change datetime into date
    df['start_date'] = df['start_date'].dt.date
    df['end_date'] = df['end_date'].dt.date

    for date in last_date_list:
        df_filt = df.copy()

        # Filter employees who are active on the last date of the quarter
        df_filt = df_filt[(df_filt['start_date'] <= date) &
                          ((df_filt['end_date'] >= date) | (df_filt['end_date'].isna()))]

        # count the number of employees who are investment and non-investment
        investment_num = df_filt[df_filt['status'] == 'Investment'].shape[0]
        non_investment_num = df_filt[df_filt['status'] == 'Non Investment'].shape[0]

        new_row = {'quarter': date, 'investment_num': investment_num, 'non_investment_num': non_investment_num}
        df_result = pd.concat([df_result, pd.DataFrame([new_row])], ignore_index=True)

    print(df_result)