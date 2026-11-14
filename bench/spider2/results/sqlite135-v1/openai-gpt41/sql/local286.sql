WITH seller_sales AS (
    SELECT
        oi.seller_id,
        SUM(oi.product_id IS NOT NULL) AS total_sales, -- total items sold
        AVG(oi.price) AS avg_item_price
    FROM order_items oi
    GROUP BY oi.seller_id
),
seller_avg_review AS (
    SELECT
        oi.seller_id,
        AVG(CAST(orv.review_score AS FLOAT)) AS avg_review_score
    FROM order_items oi
    JOIN order_reviews orv ON oi.order_id = orv.order_id
    GROUP BY oi.seller_id
),
seller_packing_time AS (
    SELECT
        oi.seller_id,
        AVG(
            JULIANDAY(o.order_approved_at) - JULIANDAY(o.order_purchase_timestamp)
        ) * 24 AS avg_packing_time_hours
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_approved_at IS NOT NULL AND o.order_purchase_timestamp IS NOT NULL
    GROUP BY oi.seller_id
),
seller_category_sales AS (
    SELECT
        oi.seller_id,
        p.product_category_name,
        COUNT(*) AS category_sales
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY oi.seller_id, p.product_category_name
),
category_max_sales AS (
    SELECT
        seller_id,
        product_category_name,
        category_sales,
        ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY category_sales DESC) as rn
    FROM seller_category_sales
)
SELECT
    s.seller_id,
    ss.total_sales,
    ROUND(ss.avg_item_price,2) AS avg_item_price,
    ROUND(sar.avg_review_score,2) AS avg_review_score,
    ROUND(spt.avg_packing_time_hours,2) AS avg_packing_time_hours,
    pct.product_category_name_english AS top_category_name,
    cms.category_sales AS top_category_sales
FROM sellers s
JOIN seller_sales ss ON s.seller_id = ss.seller_id
LEFT JOIN seller_avg_review sar ON s.seller_id = sar.seller_id
LEFT JOIN seller_packing_time spt ON s.seller_id = spt.seller_id
LEFT JOIN (
    SELECT
        cms.seller_id,
        cms.product_category_name,
        cms.category_sales
    FROM category_max_sales cms
    WHERE cms.rn = 1
) cms ON s.seller_id = cms.seller_id
LEFT JOIN product_category_name_translation pct ON cms.product_category_name = pct.product_category_name
WHERE ss.total_sales > 100
ORDER BY ss.total_sales DESC
