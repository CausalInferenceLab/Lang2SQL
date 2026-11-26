WITH purchase_visits AS (
    SELECT DISTINCT visit_id
    FROM shopping_cart_events
    WHERE event_type = 3
)
SELECT
    p.page_name AS product_name,
    COUNT(CASE WHEN e.event_type = 1 THEN 1 END) AS views,
    COUNT(CASE WHEN e.event_type = 2 THEN 1 END) AS cart_adds,
    COUNT(CASE WHEN e.event_type = 2 AND pv.visit_id IS NULL THEN 1 END) AS abandoned_cart_adds,
    COUNT(CASE WHEN e.event_type = 2 AND pv.visit_id IS NOT NULL THEN 1 END) AS purchases
FROM shopping_cart_page_hierarchy AS p
LEFT JOIN shopping_cart_events AS e
    ON e.page_id = p.page_id AND e.event_type IN (1, 2)
LEFT JOIN purchase_visits AS pv
    ON pv.visit_id = e.visit_id
WHERE p.page_id NOT IN (1, 2, 12, 13)
GROUP BY p.page_id, p.page_name
ORDER BY p.page_id;
