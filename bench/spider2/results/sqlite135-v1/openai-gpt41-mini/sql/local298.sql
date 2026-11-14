WITH member_daily_balance AS (
  SELECT
    member_id,
    date(txn_date) AS txn_day,
    SUM(CASE WHEN txn_type = 'buy' THEN quantity ELSE -quantity END) OVER (PARTITION BY member_id ORDER BY date(txn_date) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS balance
  FROM bitcoin_transactions
),
first_day_of_month AS (
  SELECT DISTINCT strftime('%Y-%m-01', txn_day) AS month_start
  FROM member_daily_balance
),
balance_on_first_of_month AS (
  SELECT
    f.month_start,
    m.member_id,
    COALESCE((SELECT balance FROM member_daily_balance b WHERE b.member_id = m.member_id AND b.txn_day <= f.month_start ORDER BY b.txn_day DESC LIMIT 1), 0) AS balance
  FROM first_day_of_month f
  CROSS JOIN (SELECT DISTINCT member_id FROM bitcoin_transactions) m
),
monthly_total_balance AS (
  SELECT
    month_start,
    SUM(CASE WHEN balance < 0 THEN 0 ELSE balance END) AS total_balance
  FROM balance_on_first_of_month
  GROUP BY month_start
  ORDER BY month_start
),
previous_month_balance AS (
  SELECT
    month_start,
    LAG(total_balance) OVER (ORDER BY month_start) AS prev_total_balance
  FROM monthly_total_balance
)
SELECT 
  month_start,
  prev_total_balance AS total_balance
FROM previous_month_balance
WHERE prev_total_balance IS NOT NULL
ORDER BY month_start ASC;
