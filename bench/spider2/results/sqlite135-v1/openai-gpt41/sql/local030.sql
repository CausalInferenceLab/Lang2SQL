WITH delivered_orders AS (
  SELECT
    o.order_id,
    c.customer_city
  FROM
    olist_orders o
    JOIN olist_customers c ON o.customer_id = c.customer_id
  WHERE
    o.order_status = 'delivered'
),
city_payments AS (
  SELECT
    do.customer_city,
    SUM(op.payment_value) AS total_payments,
    COUNT(DISTINCT do.order_id) AS delivered_order_count
  FROM
    delivered_orders do
    JOIN olist_order_payments op ON do.order_id = op.order_id
  GROUP BY
    do.customer_city
),
lowest5 AS (
  SELECT
    customer_city,
    total_payments,
    delivered_order_count
  FROM
    city_payments
  ORDER BY
    total_payments ASC
  LIMIT 5
)
SELECT
  AVG(total_payments) AS avg_total_payments,
  AVG(delivered_order_count) AS avg_delivered_order_count
FROM
  lowest5;
