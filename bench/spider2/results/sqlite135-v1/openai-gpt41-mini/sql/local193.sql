WITH customer_ltv AS (
  SELECT 
    customer_id,
    SUM(amount) AS lifetime_sales,
    MIN(payment_date) AS first_purchase_date
  FROM payment
  GROUP BY customer_id
  HAVING lifetime_sales > 0
),
customer_sales_days AS (
  SELECT
    p.customer_id,
    SUM(p.amount) AS total_sales,
    CASE WHEN p.payment_date <= DATETIME(cl.first_purchase_date, '+7 days') THEN SUM(p.amount) ELSE 0 END AS sales_7_days,
    CASE WHEN p.payment_date <= DATETIME(cl.first_purchase_date, '+30 days') THEN SUM(p.amount) ELSE 0 END AS sales_30_days
  FROM payment p
  JOIN customer_ltv cl ON p.customer_id = cl.customer_id
  GROUP BY p.customer_id, p.payment_date
),
agg_sales AS (
  SELECT
    customer_id,
    MAX(sales_7_days) AS sales_7_days,
    MAX(sales_30_days) AS sales_30_days,
    (SELECT lifetime_sales FROM customer_ltv WHERE customer_id = cs.customer_id) AS lifetime_sales
  FROM customer_sales_days cs
  GROUP BY customer_id
)
SELECT
  AVG(sales_7_days * 100.0 / lifetime_sales) AS avg_percent_7_days,
  AVG(sales_30_days * 100.0 / lifetime_sales) AS avg_percent_30_days,
  AVG(lifetime_sales) AS avg_lifetime_sales
FROM agg_sales;
