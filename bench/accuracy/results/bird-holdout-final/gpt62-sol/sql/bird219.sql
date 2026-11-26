WITH opened_accounts AS (
    SELECT a.account_id, d.district_id, d.A2 AS district_name
    FROM account AS a
    JOIN district AS d ON d.district_id = a.district_id
    WHERE d.A3 IN (SELECT A3 FROM district WHERE A2 = 'Litomerice')
      AND a.date >= '1996-01-01'
      AND a.date < '1997-01-01'
), ranked_balances AS (
    SELECT a.district_id, a.district_name, t.balance,
           ROW_NUMBER() OVER (
               PARTITION BY a.account_id
               ORDER BY t.date DESC, t.trans_id DESC
           ) AS rn
    FROM opened_accounts AS a
    LEFT JOIN trans AS t
      ON t.account_id = a.account_id AND t.date < '1997-01-01'
), year_end_balances AS (
    SELECT district_id, district_name, balance
    FROM ranked_balances
    WHERE rn = 1
), district_stats AS (
    SELECT district_name, COUNT(*) AS accounts_opened_1996,
           AVG(balance) AS avg_balance
    FROM year_end_balances
    GROUP BY district_id, district_name
)
SELECT district_name, accounts_opened_1996,
       ROUND(avg_balance, 2) AS district_avg_end_of_1996_balance,
       (SELECT ROUND(AVG(balance), 2) FROM year_end_balances)
           AS region_avg_end_of_1996_balance,
       (SELECT district_name
        FROM district_stats
        WHERE avg_balance IS NOT NULL
        ORDER BY avg_balance DESC, district_name ASC
        LIMIT 1) AS highest_average_district
FROM district_stats
ORDER BY district_name ASC;
