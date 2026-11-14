WITH event_type_map AS (
    SELECT event_type, event_name FROM shopping_cart_event_identifier
), filtered_events AS (
    SELECT p.product_id, e.event_type
    FROM shopping_cart_events e
    JOIN shopping_cart_page_hierarchy p ON e.page_id = p.page_id
    WHERE e.page_id NOT IN (1, 2, 12, 13)
), event_names AS (
    SELECT f.product_id, m.event_name
    FROM filtered_events f
    JOIN event_type_map m ON f.event_type = m.event_type
), purchase_events AS (
    SELECT p.product_id
    FROM shopping_cart_events e
    JOIN shopping_cart_page_hierarchy p ON e.page_id = p.page_id
    JOIN event_type_map m ON e.event_type = m.event_type
    WHERE m.event_name = 'purchase'
      AND e.page_id NOT IN (1, 2, 12, 13)
), view_counts AS (
    SELECT product_id, COUNT(*) AS view_count
    FROM event_names
    WHERE event_name = 'view'
    GROUP BY product_id
), add_to_cart_counts AS (
    SELECT product_id, COUNT(*) AS add_to_cart_count
    FROM event_names
    WHERE event_name = 'add_to_cart'
    GROUP BY product_id
), left_in_cart_counts AS (
    SELECT product_id, COUNT(*) AS left_in_cart_count
    FROM event_names
    WHERE event_name = 'left_in_cart'
    GROUP BY product_id
), purchase_counts AS (
    SELECT product_id, COUNT(*) AS purchase_count
    FROM purchase_events
    GROUP BY product_id
)
SELECT
    COALESCE(v.product_id, a.product_id, l.product_id, p.product_id) AS product_id,
    COALESCE(view_count, 0) AS view_count,
    COALESCE(add_to_cart_count, 0) AS add_to_cart_count,
    COALESCE(left_in_cart_count, 0) AS left_in_cart_count,
    COALESCE(purchase_count, 0) AS purchase_count
FROM view_counts v
LEFT JOIN add_to_cart_counts a ON v.product_id = a.product_id
LEFT JOIN left_in_cart_counts l ON COALESCE(v.product_id, a.product_id) = l.product_id
LEFT JOIN purchase_counts p ON COALESCE(v.product_id, a.product_id, l.product_id) = p.product_id
UNION
SELECT
    COALESCE(v.product_id, a.product_id, l.product_id, p.product_id) AS product_id,
    COALESCE(view_count, 0) AS view_count,
    COALESCE(add_to_cart_count, 0) AS add_to_cart_count,
    COALESCE(left_in_cart_count, 0) AS left_in_cart_count,
    COALESCE(purchase_count, 0) AS purchase_count
FROM add_to_cart_counts a
LEFT JOIN view_counts v ON a.product_id = v.product_id
LEFT JOIN left_in_cart_counts l ON COALESCE(v.product_id, a.product_id) = l.product_id
LEFT JOIN purchase_counts p ON COALESCE(v.product_id, a.product_id, l.product_id) = p.product_id
WHERE v.product_id IS NULL
UNION
SELECT
    COALESCE(v.product_id, a.product_id, l.product_id, p.product_id) AS product_id,
    COALESCE(view_count, 0) AS view_count,
    COALESCE(add_to_cart_count, 0) AS add_to_cart_count,
    COALESCE(left_in_cart_count, 0) AS left_in_cart_count,
    COALESCE(purchase_count, 0) AS purchase_count
FROM left_in_cart_counts l
LEFT JOIN view_counts v ON l.product_id = v.product_id
LEFT JOIN add_to_cart_counts a ON l.product_id = a.product_id
LEFT JOIN purchase_counts p ON COALESCE(v.product_id, a.product_id, l.product_id) = p.product_id
WHERE v.product_id IS NULL AND a.product_id IS NULL
UNION
SELECT
    COALESCE(v.product_id, a.product_id, l.product_id, p.product_id) AS product_id,
    COALESCE(view_count, 0) AS view_count,
    COALESCE(add_to_cart_count, 0) AS add_to_cart_count,
    COALESCE(left_in_cart_count, 0) AS left_in_cart_count,
    COALESCE(purchase_count, 0) AS purchase_count
FROM purchase_counts p
LEFT JOIN view_counts v ON p.product_id = v.product_id
LEFT JOIN add_to_cart_counts a ON p.product_id = a.product_id
LEFT JOIN left_in_cart_counts l ON p.product_id = l.product_id
WHERE v.product_id IS NULL AND a.product_id IS NULL AND l.product_id IS NULL
ORDER BY product_id
