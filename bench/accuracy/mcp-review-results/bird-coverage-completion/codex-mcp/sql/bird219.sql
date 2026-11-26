WITH ranked_transactions AS (
    SELECT
        d.district_id,
        d.A2 AS district_name,
        a.account_id,
        t.balance,
        ROW_NUMBER() OVER (
            PARTITION BY a.account_id
            ORDER BY t.date DESC, t.trans_id DESC
        ) AS transaction_rank
    FROM main.account AS a
    JOIN main.district AS d ON d.district_id = a.district_id
    LEFT JOIN main.trans AS t
        ON t.account_id = a.account_id
       AND t.date < '1997-01-01'
    WHERE a.date >= '1996-01-01'
      AND a.date < '1997-01-01'
      AND d.A3 = (SELECT A3 FROM main.district WHERE A2 = 'Litomerice')
),
account_balances AS (
    SELECT district_id, district_name, account_id, balance
    FROM ranked_transactions
    WHERE transaction_rank = 1
),
district_averages AS (
    SELECT
        district_name,
        COUNT(*) AS accounts_opened,
        AVG(balance) AS average_balance
    FROM account_balances
    GROUP BY district_id, district_name
)
SELECT
    district_name,
    accounts_opened,
    ROUND(average_balance, 2) AS district_avg_balance,
    (SELECT ROUND(AVG(balance), 2) FROM account_balances) AS region_avg_balance,
    FIRST_VALUE(district_name) OVER (
        ORDER BY average_balance DESC, district_name ASC
    ) AS highest_avg_district
FROM district_averages
ORDER BY district_name
LIMIT -1
