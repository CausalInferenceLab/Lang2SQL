WITH withdrawals_by_district AS (
    SELECT a.district_id, SUM(t.amount) AS total_withdrawn
    FROM account AS a
    JOIN trans AS t ON t.account_id = a.account_id
    WHERE t.type = 'VYDAJ'
    GROUP BY a.district_id
),
standing_orders_by_account AS (
    SELECT account_id, SUM(amount) AS total_debit
    FROM "order"
    GROUP BY account_id
),
standing_order_average_by_district AS (
    SELECT a.district_id,
           AVG(COALESCE(s.total_debit, 0.0)) AS average_debit_per_account
    FROM account AS a
    LEFT JOIN standing_orders_by_account AS s ON s.account_id = a.account_id
    GROUP BY a.district_id
),
district_metrics AS (
    SELECT d.A2 AS district_name,
           d.A3 AS region,
           CAST(d.A4 AS INTEGER) AS population,
           w.total_withdrawn,
           1.0 * w.total_withdrawn / CAST(d.A4 AS INTEGER) AS withdrawals_per_inhabitant,
           s.average_debit_per_account
    FROM district AS d
    JOIN withdrawals_by_district AS w ON w.district_id = d.district_id
    JOIN standing_order_average_by_district AS s ON s.district_id = d.district_id
    WHERE CAST(d.A4 AS INTEGER) >= 100000
)
SELECT district_name,
       region,
       population,
       total_withdrawn,
       ROUND(withdrawals_per_inhabitant, 2) AS withdrawals_per_inhabitant,
       RANK() OVER (ORDER BY withdrawals_per_inhabitant DESC) AS withdrawal_rank,
       ROUND(average_debit_per_account, 2) AS avg_standing_order_debit_per_account
FROM district_metrics
ORDER BY withdrawal_rank, district_name;
