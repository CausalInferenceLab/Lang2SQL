WITH delivered_orders AS (
    SELECT order_id 
    FROM olist_orders 
    WHERE order_status = 'delivered'
),
order_items_delivered AS (
    SELECT oi.*, o.customer_id
    FROM olist_order_items oi
    JOIN delivered_orders d ON oi.order_id = d.order_id
    JOIN olist_orders o ON oi.order_id = o.order_id
),
seller_distinct_customers AS (
    SELECT seller_id, COUNT(DISTINCT customer_id) AS distinct_customers
    FROM order_items_delivered
    GROUP BY seller_id
),
seller_profit AS (
    SELECT seller_id, SUM(price - freight_value) AS profit
    FROM order_items_delivered
    GROUP BY seller_id
),
seller_distinct_orders AS (
    SELECT seller_id, COUNT(DISTINCT order_id) AS distinct_orders
    FROM order_items_delivered
    GROUP BY seller_id
),
order_reviews_5star AS (
    SELECT order_id
    FROM olist_order_reviews
    WHERE review_score = 5
),
seller_5star_reviews AS (
    SELECT oid.seller_id, COUNT(DISTINCT oid.order_id) AS five_star_reviews
    FROM order_items_delivered oid
    JOIN order_reviews_5star r ON oid.order_id = r.order_id
    GROUP BY oid.seller_id
),
top_seller_distinct_customers AS (
    SELECT seller_id, distinct_customers, 
           'Highest number of distinct customers' AS achievement
    FROM seller_distinct_customers
    WHERE distinct_customers = (SELECT MAX(distinct_customers) FROM seller_distinct_customers)
    LIMIT 1
),
top_seller_profit AS (
    SELECT seller_id, profit,
           'Highest profit (price - freight value)' AS achievement
    FROM seller_profit
    WHERE profit = (SELECT MAX(profit) FROM seller_profit)
    LIMIT 1
),
top_seller_distinct_orders AS (
    SELECT seller_id, distinct_orders,
           'Highest number of distinct orders' AS achievement
    FROM seller_distinct_orders
    WHERE distinct_orders = (SELECT MAX(distinct_orders) FROM seller_distinct_orders)
    LIMIT 1
),
top_seller_5star_reviews AS (
    SELECT seller_id, five_star_reviews,
           'Most 5-star ratings' AS achievement
    FROM seller_5star_reviews
    WHERE five_star_reviews = (SELECT MAX(five_star_reviews) FROM seller_5star_reviews)
    LIMIT 1
)
SELECT seller_id, distinct_customers AS value, achievement
FROM top_seller_distinct_customers
UNION ALL
SELECT seller_id, profit AS value, achievement
FROM top_seller_profit
UNION ALL
SELECT seller_id, distinct_orders AS value, achievement
FROM top_seller_distinct_orders
UNION ALL
SELECT seller_id, five_star_reviews AS value, achievement
FROM top_seller_5star_reviews;
