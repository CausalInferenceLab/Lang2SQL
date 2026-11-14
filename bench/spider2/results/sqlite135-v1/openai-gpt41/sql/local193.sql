WITH first_payment AS (
  SELECT customer_id, MIN(payment_date) AS first_payment_date
  FROM payment
  GROUP BY customer_id
),
customer_ltv AS (
  SELECT 
    p.customer_id,
    fp.first_payment_date,
    SUM(p.amount) AS total_ltv,
    SUM(CASE WHEN p.payment_date <= datetime(fp.first_payment_date, '+7 days') THEN p.amount ELSE 0 END) AS ltv_7d,
    SUM(CASE WHEN p.payment_date <= datetime(fp.first_payment_date, '+30 days') THEN p.amount ELSE 0 END) AS ltv_30d
  FROM payment p
  JOIN first_payment fp ON p.customer_id = fp.customer_id
  GROUP BY p.customer_id, fp.first_payment_date
),
avg_values AS (
  SELECT
    AVG(total_ltv) AS avg_total_ltv,
    AVG(CASE WHEN total_ltv > 0 THEN (1.0 * ltv_7d / total_ltv) ELSE NULL END) AS avg_ltv_7d_pct,
    AVG(CASE WHEN total_ltv > 0 THEN (1.0 * ltv_30d / total_ltv) ELSE NULL END) AS avg_ltv_30d_pct
  FROM customer_ltv
  WHERE total_ltv > 0
)
SELECT 
  avg_total_ltv,
  avg_ltv_7d_pct AS avg_first_7d_pct_of_ltv,
  avg_ltv_30d_pct AS avg_first_30d_pct_of_ltv
FROM avg_values
