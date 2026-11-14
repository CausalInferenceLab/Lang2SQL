WITH monthly_balances AS (
  SELECT
    customer_id,
    STRFTIME('%Y-%m', txn_date) AS month,
    SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE 0 END) -
    SUM(CASE WHEN txn_type = 'withdrawal' THEN txn_amount ELSE 0 END) AS month_end_balance
  FROM customer_transactions
  WHERE txn_date BETWEEN '2020-01-01' AND '2020-12-31'
  GROUP BY customer_id, month
),
customer_counts AS (
  SELECT
    month,
    COUNT(CASE WHEN month_end_balance > 0 THEN 1 END) AS positive_balance_customers
  FROM monthly_balances
  GROUP BY month
),
extreme_months AS (
  SELECT
    (SELECT month FROM customer_counts ORDER BY positive_balance_customers DESC LIMIT 1) AS max_month,
    (SELECT month FROM customer_counts ORDER BY positive_balance_customers ASC LIMIT 1) AS min_month
),
avg_balances AS (
  SELECT
    month,
    AVG(month_end_balance) AS avg_balance
  FROM monthly_balances
  GROUP BY month
)

SELECT
  em.max_month AS month_with_highest_positive_count,
  (SELECT avg_balance FROM avg_balances WHERE month = em.max_month) AS avg_balance_highest_month,
  em.min_month AS month_with_lowest_positive_count,
  (SELECT avg_balance FROM avg_balances WHERE month = em.min_month) AS avg_balance_lowest_month,
  ((SELECT avg_balance FROM avg_balances WHERE month = em.max_month) -
  (SELECT avg_balance FROM avg_balances WHERE month = em.min_month)) AS difference_between_avg_balances
FROM extreme_months em;
