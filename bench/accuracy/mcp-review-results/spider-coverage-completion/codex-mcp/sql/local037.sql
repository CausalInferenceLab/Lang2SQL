WITH order_categories AS (
    SELECT DISTINCT i.order_id, p.product_category_name
    FROM main.olist_order_items AS i
    JOIN main.olist_products AS p ON p.product_id = i.product_id
    WHERE p.product_category_name IS NOT NULL
),
payment_counts AS (
    SELECT c.product_category_name, pay.payment_type, COUNT(*) AS payment_count
    FROM order_categories AS c
    JOIN main.olist_order_payments AS pay ON pay.order_id = c.order_id
    WHERE pay.payment_type IS NOT NULL
    GROUP BY c.product_category_name, pay.payment_type
),
ranked_types AS (
    SELECT product_category_name, payment_type, payment_count,
           ROW_NUMBER() OVER (
               PARTITION BY product_category_name
               ORDER BY payment_count DESC, payment_type
           ) AS type_rank
    FROM payment_counts
)
SELECT product_category_name, payment_type, payment_count
FROM ranked_types
WHERE type_rank = 1
ORDER BY payment_count DESC, product_category_name
LIMIT 3;
