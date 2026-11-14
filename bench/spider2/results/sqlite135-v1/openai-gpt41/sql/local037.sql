WITH product_category_orders AS (
  SELECT
    p.product_category_name,
    oi.order_id
  FROM olist_order_items oi
  JOIN olist_products p ON oi.product_id = p.product_id
),
order_payments_with_cat AS (
  SELECT
    pco.product_category_name,
    op.payment_type,
    op.payment_sequential
  FROM olist_order_payments op
  JOIN product_category_orders pco ON op.order_id = pco.order_id
),
category_payment_counts AS (
  SELECT
    product_category_name,
    payment_type,
    COUNT(*) AS num_payments
  FROM order_payments_with_cat
  GROUP BY product_category_name, payment_type
),
category_top_payment AS (
  SELECT
    product_category_name,
    payment_type,
    num_payments,
    ROW_NUMBER() OVER (
      PARTITION BY product_category_name
      ORDER BY num_payments DESC, payment_type
    ) AS rn
  FROM category_payment_counts
)
SELECT
  product_category_name,
  payment_type AS most_common_payment_type,
  num_payments
FROM category_top_payment
WHERE rn = 1
ORDER BY num_payments DESC
LIMIT 3;
