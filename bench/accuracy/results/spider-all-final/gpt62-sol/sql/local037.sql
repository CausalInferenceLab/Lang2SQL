WITH order_categories AS (
    SELECT DISTINCT oi.order_id, p.product_category_name
    FROM olist_order_items AS oi
    JOIN olist_products AS p ON p.product_id = oi.product_id
    WHERE p.product_category_name IS NOT NULL
), payment_counts AS (
    SELECT oc.product_category_name, op.payment_type, COUNT(*) AS payment_count
    FROM order_categories AS oc
    JOIN olist_order_payments AS op ON op.order_id = oc.order_id
    GROUP BY oc.product_category_name, op.payment_type
), ranked AS (
    SELECT product_category_name, payment_type, payment_count,
           ROW_NUMBER() OVER (
               PARTITION BY product_category_name
               ORDER BY payment_count DESC, payment_type
           ) AS payment_type_rank
    FROM payment_counts
)
SELECT COALESCE(t.product_category_name_english, r.product_category_name) AS product_category,
       r.payment_type,
       r.payment_count
FROM ranked AS r
LEFT JOIN product_category_name_translation AS t
    ON t.product_category_name = r.product_category_name
WHERE r.payment_type_rank = 1
ORDER BY r.payment_count DESC, r.product_category_name
LIMIT 3;
