WITH order_categories AS (
    SELECT DISTINCT oi.order_id, p.product_category_name
    FROM olist_order_items AS oi
    JOIN olist_products AS p ON p.product_id = oi.product_id
), payment_counts AS (
    SELECT oc.product_category_name, op.payment_type, COUNT(*) AS payment_count
    FROM order_categories AS oc
    JOIN olist_order_payments AS op ON op.order_id = oc.order_id
    GROUP BY oc.product_category_name, op.payment_type
), category_maxima AS (
    SELECT product_category_name, MAX(payment_count) AS preferred_payment_count
    FROM payment_counts
    GROUP BY product_category_name
)
SELECT AVG(preferred_payment_count) AS average_preferred_payment_count
FROM category_maxima;
