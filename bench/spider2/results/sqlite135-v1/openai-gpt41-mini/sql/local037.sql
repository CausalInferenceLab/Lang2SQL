WITH CategoryPaymentTypeCounts AS (
    SELECT
        p.product_category_name,
        pay.payment_type,
        COUNT(*) AS payment_count
    FROM olist_order_items oi
    JOIN olist_order_payments pay ON oi.order_id = pay.order_id
    JOIN olist_products p ON oi.product_id = p.product_id
    GROUP BY p.product_category_name, pay.payment_type
),
CategoryMaxPaymentType AS (
    SELECT
        product_category_name,
        payment_type,
        payment_count,
        RANK() OVER (PARTITION BY product_category_name ORDER BY payment_count DESC) AS rank
    FROM CategoryPaymentTypeCounts
),
MostCommonPaymentPerCategory AS (
    SELECT
        product_category_name,
        payment_type,
        payment_count
    FROM CategoryMaxPaymentType
    WHERE rank = 1
),
TopThreeCategories AS (
    SELECT
        product_category_name,
        payment_type,
        payment_count
    FROM MostCommonPaymentPerCategory
    ORDER BY payment_count DESC
    LIMIT 3
)
SELECT * FROM TopThreeCategories;
