import pandas as pd


if __name__ == '__main__':
    # put csv into pandas column entry_date as datetime
    df = pd.read_csv('Pragma\df_pnl.csv', parse_dates=['entry_date'])
    # group by YYYY-MM and ticker and sum pnl_usd
    df_group = df.groupby([df['YYYYMM'], 'ticker'])['pnl_usd'].sum().reset_index()
    # save df group into excel
    df_group.to_excel(r'Pragma\df_group.xlsx', index=False)





    pass