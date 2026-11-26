WITH seller_orders AS (
    SELECT i.seller_id, o.order_id, o.customer_id,
           SUM(i.price - i.freight_value) AS order_profit
    FROM main.olist_orders AS o
    JOIN main.olist_order_items AS i ON i.order_id = o.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY i.seller_id, o.order_id, o.customer_id
),
five_star_ratings AS (
    SELECT order_id, COUNT(*) AS rating_count
    FROM main.olist_order_reviews
    WHERE review_score = 5
    GROUP BY order_id
),
seller_totals AS (
    SELECT s.seller_id,
           COUNT(DISTINCT c.customer_unique_id) AS customers,
           ROUND(SUM(s.order_profit), 2) AS profit,
           COUNT(DISTINCT s.order_id) AS orders,
           SUM(COALESCE(r.rating_count, 0)) AS ratings
    FROM seller_orders AS s
    LEFT JOIN main.olist_customers AS c ON c.customer_id = s.customer_id
    LEFT JOIN five_star_ratings AS r ON r.order_id = s.order_id
    GROUP BY s.seller_id
),
achievements AS (
    SELECT 1 AS category, 'Most distinct customers' AS achievement,
           seller_id, customers AS value
    FROM seller_totals
    UNION ALL
    SELECT 2, 'Highest total profit', seller_id, profit FROM seller_totals
    UNION ALL
    SELECT 3, 'Most distinct orders', seller_id, orders FROM seller_totals
    UNION ALL
    SELECT 4, 'Most 5-star ratings', seller_id, ratings FROM seller_totals
),
ranked AS (
    SELECT achievements.*,
           RANK() OVER (PARTITION BY category ORDER BY value DESC) AS position
    FROM achievements
)
SELECT achievement, seller_id, value
FROM ranked
WHERE position = 1
ORDER BY category, seller_id
LIMIT 1000
