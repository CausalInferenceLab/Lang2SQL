WITH filtered_events AS (
    SELECT
        sce.event_id,
        sce.page_id,
        sce.product_id,
        sce.event_type,
        sce.user_id
    FROM shopping_cart_events sce
    WHERE sce.page_id NOT IN (1, 2, 12, 13)
),

view_events AS (
    SELECT
        product_id,
        COUNT(*) AS view_count
    FROM filtered_events
    WHERE event_type = 'view'
    GROUP BY product_id
),

add_to_cart_events AS (
    SELECT
        product_id,
        COUNT(*) AS add_to_cart_count
    FROM filtered_events
    WHERE event_type = 'add_to_cart'
    GROUP BY product_id
),

purchased_events AS (
    SELECT
        product_id,
        COUNT(*) AS purchase_count
    FROM filtered_events
    WHERE event_type = 'purchase'
    GROUP BY product_id
),

cart_left_events AS (
    -- Users who added to cart but never purchased per product
    SELECT
        a.product_id,
        COUNT(DISTINCT a.user_id) AS left_in_cart_count
    FROM filtered_events a
    LEFT JOIN filtered_events p
        ON a.product_id = p.product_id 
        AND a.user_id = p.user_id
        AND p.event_type = 'purchase'
    WHERE a.event_type = 'add_to_cart'
      AND p.event_id IS NULL
    GROUP BY a.product_id
)

SELECT
    COALESCE(v.product_id, a.product_id, p.product_id, c.product_id) AS product_id,
    COALESCE(v.view_count, 0) AS view_count,
    COALESCE(a.add_to_cart_count, 0) AS add_to_cart_count,
    COALESCE(c.left_in_cart_count, 0) AS left_in_cart_without_purchase_count,
    COALESCE(p.purchase_count, 0) AS purchase_count
FROM view_events v
FULL OUTER JOIN add_to_cart_events a ON v.product_id = a.product_id
FULL OUTER JOIN purchased_events p ON COALESCE(v.product_id, a.product_id) = p.product_id
FULL OUTER JOIN cart_left_events c ON COALESCE(v.product_id, a.product_id, p.product_id) = c.product_id
ORDER BY product_id;
