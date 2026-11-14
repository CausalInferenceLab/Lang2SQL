WITH delivered_orders AS (
    SELECT order_id
    FROM olist_orders
    WHERE order_status = 'delivered'
),

seller_customer_ids AS (
    SELECT oi.seller_id, COUNT(DISTINCT c.customer_unique_id) AS unique_customers
    FROM olist_order_items oi
    JOIN delivered_orders d ON oi.order_id = d.order_id
    JOIN olist_orders o ON oi.order_id = o.order_id
    JOIN olist_customers c ON o.customer_id = c.customer_id
    GROUP BY oi.seller_id
),

seller_profits AS (
    SELECT seller_id, SUM(price - freight_value) AS total_profit
    FROM olist_order_items oi
    JOIN delivered_orders d ON oi.order_id = d.order_id
    GROUP BY seller_id
),

seller_orders AS (
    SELECT oi.seller_id, COUNT(DISTINCT oi.order_id) AS distinct_orders
    FROM olist_order_items oi
    JOIN delivered_orders d ON oi.order_id = d.order_id
    GROUP BY oi.seller_id
),

seller_5star_ratings AS (
    SELECT oi.seller_id, COUNT(*) AS five_star_count
    FROM olist_order_reviews r
    JOIN olist_order_items oi ON r.order_id = oi.order_id
    JOIN delivered_orders d ON oi.order_id = d.order_id
    WHERE r.review_score = 5
    GROUP BY oi.seller_id
),

max_unique_customers AS (
    SELECT 'Seller with highest distinct customers' AS category, seller_id, unique_customers AS value
    FROM seller_customer_ids
    ORDER BY unique_customers DESC
    LIMIT 1
),

max_profit AS (
    SELECT 'Seller with highest profit' AS category, seller_id, total_profit AS value
    FROM seller_profits
    ORDER BY total_profit DESC
    LIMIT 1
),

max_orders AS (
    SELECT 'Seller with highest distinct orders' AS category, seller_id, distinct_orders AS value
    FROM seller_orders
    ORDER BY distinct_orders DESC
    LIMIT 1
),

max_five_star_ratings AS (
    SELECT 'Seller with most 5-star ratings' AS category, seller_id, five_star_count AS value
    FROM seller_5star_ratings
    ORDER BY five_star_count DESC
    LIMIT 1
)

SELECT * FROM max_unique_customers
UNION ALL
SELECT * FROM max_profit
UNION ALL
SELECT * FROM max_orders
UNION ALL
SELECT * FROM max_five_star_ratings;
