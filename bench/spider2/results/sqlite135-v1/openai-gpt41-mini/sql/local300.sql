WITH daily_balances AS (
  SELECT
    customer_id,
    date(txn_date) AS date,
    SUM(txn_amount) OVER (PARTITION BY customer_id ORDER BY date(txn_date) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS raw_balance
  FROM customer_transactions
),
filled_dates AS (
  SELECT
    customer_id,
    date(min_date, '+' || delta || ' day') AS date
  FROM (
    SELECT
      customer_id,
      MIN(date(txn_date)) AS min_date,
      MAX(date(txn_date)) AS max_date
    FROM customer_transactions
    GROUP BY customer_id
  ),
  (SELECT 0 AS delta UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
   UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20
   UNION ALL SELECT 21 UNION ALL SELECT 22 UNION ALL SELECT 23 UNION ALL SELECT 24 UNION ALL SELECT 25 UNION ALL SELECT 26 UNION ALL SELECT 27 UNION ALL SELECT 28 UNION ALL SELECT 29 UNION ALL SELECT 30
   UNION ALL SELECT 31 UNION ALL SELECT 32 UNION ALL SELECT 33 UNION ALL SELECT 34 UNION ALL SELECT 35 UNION ALL SELECT 36 UNION ALL SELECT 37 UNION ALL SELECT 38 UNION ALL SELECT 39 UNION ALL SELECT 40
   UNION ALL SELECT 41 UNION ALL SELECT 42 UNION ALL SELECT 43 UNION ALL SELECT 44 UNION ALL SELECT 45 UNION ALL SELECT 46 UNION ALL SELECT 47 UNION ALL SELECT 48 UNION ALL SELECT 49 UNION ALL SELECT 50
   UNION ALL SELECT 51 UNION ALL SELECT 52 UNION ALL SELECT 53 UNION ALL SELECT 54 UNION ALL SELECT 55 UNION ALL SELECT 56 UNION ALL SELECT 57 UNION ALL SELECT 58 UNION ALL SELECT 59 UNION ALL SELECT 60) AS days
  WHERE date(min_date, '+' || delta || ' day') <= max_date
),
daily_balances_filled AS (
  SELECT
    fd.customer_id,
    fd.date,
    COALESCE(db.raw_balance, 0) AS raw_balance
  FROM filled_dates fd
  LEFT JOIN daily_balances db ON fd.customer_id = db.customer_id AND fd.date = db.date
),
carried_balances AS (
  SELECT
    customer_id,
    date,
    CASE WHEN raw_balance = 0 THEN NULL ELSE raw_balance END AS balance_if_present
  FROM daily_balances_filled
),
carry_forward AS (
  SELECT
    customer_id,
    date,
    MAX(balance_if_present) OVER (PARTITION BY customer_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS balance
  FROM carried_balances
),
non_negative_balances AS (
  SELECT
    customer_id,
    date,
    CASE WHEN balance < 0 THEN 0 ELSE balance END AS balance
  FROM carry_forward
),
monthly_max_balances AS (
  SELECT
    customer_id,
    strftime('%Y-%m', date) AS month,
    MAX(balance) AS max_balance
  FROM non_negative_balances
  GROUP BY customer_id, strftime('%Y-%m', date)
),
monthly_total AS (
  SELECT
    month,
    SUM(max_balance) AS total_max_balance
  FROM monthly_max_balances
  GROUP BY month
  ORDER BY month
)
SELECT * FROM monthly_total;
