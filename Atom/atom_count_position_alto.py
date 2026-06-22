
from models import engine
import pandas as pd


def get_monthly_long_short_count():
    my_sql =f"""SELECT 
        p.entry_date,
        SUM(CASE WHEN p.quantity > 0 THEN 1 ELSE 0 END) AS long_count,
        SUM(CASE WHEN p.quantity < 0 THEN 1 ELSE 0 END) AS short_count
    FROM position p
    JOIN (
        SELECT 
            MAX(entry_date) AS entry_date
        FROM position
        WHERE parent_fund_id = 1
          AND entry_date > '2019-04-01' and entry_date<'2026-06-01'
        GROUP BY DATE_FORMAT(entry_date, '%%Y-%%m')
    ) m
        ON p.entry_date = m.entry_date
    WHERE p.parent_fund_id = 1
    GROUP BY p.entry_date
    ORDER BY p.entry_date;"""

    df_ls = pd.read_sql(my_sql, con=engine)
    df_ls['total_count'] = df_ls['long_count'] + df_ls['short_count']
    # export into excel
    df_ls.to_excel('Monthly_Long_Short_Count.xlsx', index=False)


if __name__ == "__main__":
    get_monthly_long_short_count()
