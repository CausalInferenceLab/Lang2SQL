WITH month_txn AS (
  SELECT 
    customer_id,
    strftime('%Y-%m', txn_date) AS year_month,
    SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE 0 END) - 
      SUM(CASE WHEN txn_type = 'withdrawal' THEN txn_amount ELSE 0 END) AS month_end_balance
  FROM customer_transactions
  WHERE strftime('%Y', txn_date) = '2020'
  GROUP BY customer_id, year_month
),
month_positive_counts AS (
  SELECT 
    year_month,
    COUNT(DISTINCT CASE WHEN month_end_balance > 0 THEN customer_id END) AS pos_bal_count
  FROM month_txn
  GROUP BY year_month
),
max_min_months AS (
  SELECT 
    MAX(pos_bal_count) AS max_count,
    MIN(pos_bal_count) AS min_count
  FROM month_positive_counts
),
max_month AS (
  SELECT year_month, pos_bal_count
  FROM month_positive_counts, max_min_months
  WHERE pos_bal_count = max_count
  LIMIT 1
),
min_month AS (
  SELECT year_month, pos_bal_count
  FROM month_positive_counts, max_min_months
  WHERE pos_bal_count = min_count
  LIMIT 1
),
month_avgs AS (
  SELECT 
    year_month, 
    AVG(month_end_balance * 1.0) AS avg_month_end_balance
  FROM month_txn
  GROUP BY year_month
)
SELECT 
  max_month.year_month AS month_with_most_positive,
  min_month.year_month AS month_with_least_positive,
  max_avg.avg_month_end_balance AS avg_balance_in_most_positive_month,
  min_avg.avg_month_end_balance AS avg_balance_in_least_positive_month,
  (max_avg.avg_month_end_balance - min_avg.avg_month_end_balance) AS avg_difference
FROM
  max_month
  JOIN min_month
  JOIN month_avgs AS max_avg ON max_avg.year_month = max_month.year_month
  JOIN month_avgs AS min_avg ON min_avg.year_month = min_month.year_month
