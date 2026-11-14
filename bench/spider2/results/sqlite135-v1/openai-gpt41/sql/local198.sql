WITH customer_counts AS (
  SELECT country, COUNT(*) AS num_customers
  FROM customers
  GROUP BY country
),
country_sales AS (
  SELECT c.country, SUM(i.total) AS total_sales
  FROM customers c
  JOIN invoices i ON c.customer_id = i.customer_id
  GROUP BY c.country
),
filtered_sales AS (
  SELECT cs.total_sales
  FROM country_sales cs
  JOIN customer_counts cc ON cs.country = cc.country
  WHERE cc.num_customers > 4
)
SELECT
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_sales) AS median_total_sales
FROM filtered_sales;
