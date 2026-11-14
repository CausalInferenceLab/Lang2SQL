WITH monthly_payments AS (
  SELECT
    customer_id,
    strftime('%Y-%m', payment_date) AS year_month,
    SUM(amount) AS total_amount
  FROM payment
  GROUP BY customer_id, year_month
),
monthly_changes AS (
  SELECT
    customer_id,
    year_month,
    total_amount - LAG(total_amount) OVER (PARTITION BY customer_id ORDER BY year_month) AS monthly_change
  FROM monthly_payments
),
avg_monthly_change AS (
  SELECT
    customer_id,
    AVG(ABS(monthly_change)) AS avg_change
  FROM monthly_changes
  WHERE monthly_change IS NOT NULL
  GROUP BY customer_id
),
max_change_customer AS (
  SELECT customer_id
  FROM avg_monthly_change
  ORDER BY avg_change DESC
  LIMIT 1
)
SELECT c.first_name || ' ' || c.last_name AS full_name
FROM customer c
JOIN max_change_customer mcc ON c.customer_id = mcc.customer_id;
