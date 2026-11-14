WITH category_payment_counts AS (
    SELECT
        p.product_category_name,
        op.payment_type,
        COUNT(*) AS num_payments
    FROM
        olist_order_items oi
        JOIN olist_products p ON oi.product_id = p.product_id
        JOIN olist_order_payments op ON oi.order_id = op.order_id
    GROUP BY
        p.product_category_name,
        op.payment_type
),
most_preferred_payments AS (
    SELECT
        category_payment_counts.product_category_name,
        category_payment_counts.payment_type,
        category_payment_counts.num_payments
    FROM
        category_payment_counts
        JOIN (
            SELECT
                product_category_name,
                MAX(num_payments) AS max_payments
            FROM
                category_payment_counts
            GROUP BY
                product_category_name
        ) max_per_category
        ON category_payment_counts.product_category_name = max_per_category.product_category_name
        AND category_payment_counts.num_payments = max_per_category.max_payments
)
SELECT
    AVG(num_payments) AS avg_num_payments_for_top_method
FROM
    most_preferred_payments;
