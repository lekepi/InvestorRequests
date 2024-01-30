


if __name__ == '__main__':
    print(1)

    # turnover by capital
    # calculate it day by day against the leveraged AUM of the month minus the trade done for readjustment:
    # that means if the long goes from200 to 210 M between last day of the month and first day of next month, remove the differenc ebetween the 2 from the trading


    # turnover by name: review email


    # turnover by strategy: review email# old method:
    # SELECT avg(amount) FROM anandaprod.aum WHERE type='leveraged' AND year(entry_date)=2023;
    # SELECT sum(abs(notional_usd)) FROM trade T1 JOIN product T2 on T1.product_id=T2.id where prod_type in ('Cash') and still_active=1 and year(trade_date)='2023';
