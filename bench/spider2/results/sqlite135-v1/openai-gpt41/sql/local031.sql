WITH delivered_orders AS (
  SELECT
    order_id,
    order_delivered_customer_date,
    strftime('%Y', order_delivered_customer_date) AS year,
    strftime('%Y-%m', order_delivered_customer_date) AS year_month
  FROM olist_orders
  WHERE order_delivered_customer_date IS NOT NULL
    AND strftime('%Y', order_delivered_customer_date) IN ('2016', '2017', '2018')
),
annual_volumes AS (
  SELECT
    year,
    COUNT(*) AS annual_delivered_orders
  FROM delivered_orders
  GROUP BY year
),
lowest_year AS (
  SELECT year
  FROM annual_volumes
  ORDER BY annual_delivered_orders ASC
  LIMIT 1
),
monthly_volumes AS (
  SELECT
    year_month,
    COUNT(*) AS monthly_delivered_orders
  FROM delivered_orders
  WHERE year = (SELECT year FROM lowest_year)
  GROUP BY year_month
)
SELECT
  MAX(monthly_delivered_orders) AS highest_monthly_delivered_orders_in_lowest_year
FROM monthly_volumes;
