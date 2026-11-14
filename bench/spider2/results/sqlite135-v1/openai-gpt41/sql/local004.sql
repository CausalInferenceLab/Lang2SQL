WITH customer_orders AS (
  SELECT
    o.customer_id,
    o.order_id,
    MIN(o.order_purchase_timestamp) OVER (PARTITION BY o.customer_id) AS first_purchase,
    MAX(o.order_purchase_timestamp) OVER (PARTITION BY o.customer_id) AS last_purchase
  FROM orders o
),
customer_payments AS (
  SELECT
    co.customer_id,
    COUNT(DISTINCT co.order_id) AS num_orders,
    SUM(op.payment_value) AS total_payment,
    MIN(co.first_purchase) AS first_purchase,
    MAX(co.last_purchase) AS last_purchase
  FROM customer_orders co
  JOIN order_payments op ON co.order_id = op.order_id
  GROUP BY co.customer_id
),
customer_metrics AS (
  SELECT
    cp.customer_id,
    cp.num_orders,
    (1.0 * cp.total_payment) / cp.num_orders AS avg_payment_per_order,
    -- Lifespan in weeks
    CASE 
      WHEN (JULIANDAY(cp.last_purchase) - JULIANDAY(cp.first_purchase)) / 7.0 < 1.0 
        THEN 1.0
      ELSE (JULIANDAY(cp.last_purchase) - JULIANDAY(cp.first_purchase)) / 7.0 
    END AS lifespan_weeks
  FROM customer_payments cp
  WHERE cp.num_orders > 0
)
SELECT
  cm.customer_id,
  cm.num_orders,
  cm.avg_payment_per_order,
  cm.lifespan_weeks
FROM customer_metrics cm
ORDER BY cm.avg_payment_per_order DESC
LIMIT 3;
