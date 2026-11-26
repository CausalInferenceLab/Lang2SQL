WITH seller_orders AS (
    SELECT i.seller_id,
           o.order_id,
           o.customer_id,
           SUM(i.price - i.freight_value) AS order_profit
    FROM olist_orders AS o
    JOIN olist_order_items AS i ON i.order_id = o.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY i.seller_id, o.order_id, o.customer_id
), five_star_reviews AS (
    SELECT order_id, COUNT(*) AS rating_count
    FROM olist_order_reviews
    WHERE review_score = 5
    GROUP BY order_id
), seller_totals AS (
    SELECT so.seller_id,
           COUNT(DISTINCT c.customer_unique_id) AS customer_count,
           SUM(so.order_profit) AS profit,
           COUNT(DISTINCT so.order_id) AS order_count,
           SUM(COALESCE(r.rating_count, 0)) AS five_star_count
    FROM seller_orders AS so
    LEFT JOIN olist_customers AS c ON c.customer_id = so.customer_id
    LEFT JOIN five_star_reviews AS r ON r.order_id = so.order_id
    GROUP BY so.seller_id
), ranked AS (
    SELECT seller_id, customer_count, profit, order_count, five_star_count,
           ROW_NUMBER() OVER (ORDER BY customer_count DESC, seller_id) AS customer_rank,
           ROW_NUMBER() OVER (ORDER BY profit DESC, seller_id) AS profit_rank,
           ROW_NUMBER() OVER (ORDER BY order_count DESC, seller_id) AS order_rank,
           ROW_NUMBER() OVER (ORDER BY five_star_count DESC, seller_id) AS rating_rank
    FROM seller_totals
), achievements AS (
    SELECT 1 AS position, 'Most distinct customer unique IDs' AS achievement,
           seller_id, customer_count AS value
    FROM ranked WHERE customer_rank = 1
    UNION ALL
    SELECT 2, 'Highest profit (price minus freight value)', seller_id, ROUND(profit, 2)
    FROM ranked WHERE profit_rank = 1
    UNION ALL
    SELECT 3, 'Most distinct orders', seller_id, order_count
    FROM ranked WHERE order_rank = 1
    UNION ALL
    SELECT 4, 'Most 5-star ratings', seller_id, five_star_count
    FROM ranked WHERE rating_rank = 1
)
SELECT achievement, seller_id, value
FROM achievements
ORDER BY position;
