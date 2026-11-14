WITH italian_customers AS (
  SELECT customer_id
  FROM customers c
  JOIN countries co ON c.country_id = co.country_id
  WHERE co.country_name = 'Italy'
),
december_2021_sales AS (
  SELECT
    s.customer_id,
    (s.quantity_sold * (s.unit_price - s.unit_cost)) AS profit,
    t.month,
    t.year
  FROM sales s
  JOIN times t ON s.time_id = t.time_id
  WHERE t.year = 2021 AND t.month = 12
    AND s.customer_id IN (SELECT customer_id FROM italian_customers)
),
customer_monthly_profits AS (
  SELECT
    customer_id,
    SUM(profit) AS total_profit
  FROM december_2021_sales
  GROUP BY customer_id
),
profit_bounds AS (
  SELECT
    MIN(total_profit) AS min_profit,
    MAX(total_profit) AS max_profit
  FROM customer_monthly_profits
),
bucketed_customers AS (
  SELECT
    cmp.customer_id,
    cmp.total_profit,
    -- Compute bucket index from 1 to 10
    1 + CAST(
      (CASE
         WHEN pb.max_profit = pb.min_profit THEN 0
         ELSE ((cmp.total_profit - pb.min_profit) * 10.0) / (pb.max_profit - pb.min_profit)
       END
      ) AS INTEGER
    ) AS bucket
  FROM customer_monthly_profits cmp
  CROSS JOIN profit_bounds pb
)
SELECT
  bucket,
  COUNT(DISTINCT customer_id) AS num_customers,
  MIN(total_profit) AS min_total_profit,
  MAX(total_profit) AS max_total_profit
FROM bucketed_customers
GROUP BY bucket
ORDER BY bucket
