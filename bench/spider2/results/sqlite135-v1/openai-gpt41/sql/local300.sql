WITH date_bounds AS (
  SELECT 
    customer_id,
    MIN(date(txn_date)) AS min_date,
    MAX(date(txn_date)) AS max_date
  FROM customer_transactions
  GROUP BY customer_id
),
all_dates AS (
  SELECT customer_id, date(min_date, '+' || n || ' days') as the_date
  FROM date_bounds, 
       (SELECT 0 as n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20 UNION ALL SELECT 21 UNION ALL SELECT 22 UNION ALL SELECT 23 UNION ALL SELECT 24 UNION ALL SELECT 25 UNION ALL SELECT 26 UNION ALL SELECT 27 UNION ALL SELECT 28 UNION ALL SELECT 29 UNION ALL SELECT 30 UNION ALL SELECT 31 UNION ALL SELECT 32 UNION ALL SELECT 33 UNION ALL SELECT 34 UNION ALL SELECT 35 UNION ALL SELECT 36 UNION ALL SELECT 37 UNION ALL SELECT 38 UNION ALL SELECT 39 UNION ALL SELECT 40 UNION ALL SELECT 41 UNION ALL SELECT 42 UNION ALL SELECT 43 UNION ALL SELECT 44 UNION ALL SELECT 45 UNION ALL SELECT 46 UNION ALL SELECT 47 UNION ALL SELECT 48 UNION ALL SELECT 49) nums
  WHERE date(min_date, '+' || n || ' days') <= max_date
),
per_day_trans AS (
  SELECT customer_id, date(txn_date) as the_date, SUM(txn_amount) as day_amount
  FROM customer_transactions
  GROUP BY customer_id, the_date
),
joined_dates AS (
  SELECT d.customer_id, d.the_date, COALESCE(t.day_amount, 0) as day_amount
  FROM all_dates d
  LEFT JOIN per_day_trans t ON d.customer_id = t.customer_id AND d.the_date = t.the_date
),
customer_balances AS (
  SELECT 
    customer_id,
    the_date,
    MAX(SUM(day_amount) OVER (PARTITION BY customer_id ORDER BY the_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) as balance
  FROM joined_dates
),
customer_balances_nonneg AS (
  SELECT customer_id, the_date, CASE WHEN balance < 0 THEN 0 ELSE balance END as balance
  FROM customer_balances
),
max_daily_balance_per_month AS (
  SELECT 
    customer_id,
    strftime('%Y-%m', the_date) as year_month,
    MAX(balance) as max_balance
  FROM customer_balances_nonneg
  GROUP BY customer_id, year_month
)
SELECT
  year_month,
  SUM(max_balance) as total_max_daily_balance
FROM max_daily_balance_per_month
GROUP BY year_month
ORDER BY year_month
