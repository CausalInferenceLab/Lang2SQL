WITH category_payment_counts AS (
  SELECT
    p.product_category_name,
    op.payment_type,
    COUNT(*) AS payment_count
  FROM olist_order_items oi
  JOIN olist_products p ON oi.product_id = p.product_id
  JOIN olist_orders o ON oi.order_id = o.order_id
  JOIN olist_order_payments op ON o.order_id = op.order_id
  GROUP BY p.product_category_name, op.payment_type
),
most_preferred_payment AS (
  SELECT
    product_category_name,
    payment_type,
    payment_count,
    RANK() OVER (PARTITION BY product_category_name ORDER BY payment_count DESC) AS rank
  FROM category_payment_counts
),
most_preferred_payment_filtered AS (
  SELECT product_category_name, payment_type, payment_count
  FROM most_preferred_payment
  WHERE rank = 1
)
SELECT AVG(payment_count) AS avg_total_payments
FROM most_preferred_payment_filtered;
