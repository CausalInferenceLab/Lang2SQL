WITH delivered_orders AS (
  SELECT
    o.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    COUNT(*) AS num_delivered_orders
  FROM olist_orders o
  JOIN olist_customers c ON o.customer_id = c.customer_id
  WHERE o.order_status = 'delivered'
  GROUP BY o.customer_id, c.customer_unique_id, c.customer_city, c.customer_state
),
top_customers AS (
  SELECT
    customer_unique_id,
    customer_city,
    customer_state,
    num_delivered_orders
  FROM delivered_orders
  ORDER BY num_delivered_orders DESC
  LIMIT 3
)
SELECT
  tc.customer_unique_id,
  tc.customer_city,
  tc.customer_state,
  do.num_delivered_orders,
  AVG(oi_total.payment_value) AS avg_payment_value
FROM top_customers tc
JOIN delivered_orders do
  ON tc.customer_unique_id = do.customer_unique_id
JOIN olist_orders o
  ON do.customer_id = o.customer_id
JOIN (
  SELECT
    order_id,
    SUM(payment_value) AS payment_value
  FROM olist_order_payments
  GROUP BY order_id
) oi_total ON o.order_id = oi_total.order_id
WHERE o.order_status = 'delivered'
GROUP BY tc.customer_unique_id, tc.customer_city, tc.customer_state, do.num_delivered_orders
ORDER BY do.num_delivered_orders DESC
LIMIT 3;
