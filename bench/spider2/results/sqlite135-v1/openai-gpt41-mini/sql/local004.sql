WITH customer_payments AS (
  SELECT
    o.customer_id,
    COUNT(DISTINCT o.order_id) AS number_of_orders,
    AVG(op.payment_value) AS avg_payment_per_order,
    MIN(o.order_purchase_timestamp) AS first_order_date,
    MAX(o.order_purchase_timestamp) AS last_order_date
  FROM orders o
  JOIN order_payments op ON o.order_id = op.order_id
  GROUP BY o.customer_id
),
customer_lifespans AS (
  SELECT
    customer_id,
    number_of_orders,
    avg_payment_per_order,
    CASE
      WHEN (julianday(last_order_date) - julianday(first_order_date))/7 < 1 THEN 1.0
      ELSE (julianday(last_order_date) - julianday(first_order_date))/7
    END AS lifespan_in_weeks
  FROM customer_payments
)
SELECT
  customer_id,
  number_of_orders,
  avg_payment_per_order,
  lifespan_in_weeks
FROM customer_lifespans
ORDER BY avg_payment_per_order DESC
LIMIT 3;
