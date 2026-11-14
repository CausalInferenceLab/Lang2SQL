WITH SellerSales AS (
  SELECT 
    oi.seller_id,
    SUM(oi.price) AS total_sales,
    AVG(oi.price) AS avg_item_price,
    SUM(oi.order_item_id) AS total_quantity
  FROM order_items oi
  GROUP BY oi.seller_id
  HAVING SUM(oi.order_item_id) > 100
),
SellerReviews AS (
  SELECT 
    oi.seller_id,
    AVG(orv.review_score) AS avg_review_score
  FROM order_items oi
  JOIN order_reviews orv ON oi.order_id = orv.order_id
  GROUP BY oi.seller_id
),
SellerPackingTimes AS (
  SELECT 
    oi.seller_id,
    AVG(julianday(o.order_delivered_customer_date) - julianday(o.order_approved_at)) AS avg_packing_time
  FROM order_items oi
  JOIN orders o ON oi.order_id = o.order_id
  GROUP BY oi.seller_id
),
SellerCategorySales AS (
  SELECT 
    oi.seller_id,
    p.product_category_name,
    SUM(oi.order_item_id) AS category_quantity
  FROM order_items oi
  JOIN products p ON oi.product_id = p.product_id
  GROUP BY oi.seller_id, p.product_category_name
),
MaxCategorySales AS (
  SELECT 
    seller_id,
    MAX(category_quantity) AS max_quantity
  FROM SellerCategorySales
  GROUP BY seller_id
),
TopCategoryPerSeller AS (
  SELECT s.seller_id, scs.product_category_name
  FROM MaxCategorySales s
  JOIN SellerCategorySales scs ON s.seller_id = scs.seller_id AND s.max_quantity = scs.category_quantity
),
CategoryNameEnglish AS (
  SELECT pcnt.product_category_name, pcnt.product_category_name_english
  FROM product_category_name_translation pcnt
)
SELECT
  ss.seller_id,
  ss.total_sales,
  ss.avg_item_price,
  sr.avg_review_score,
  spt.avg_packing_time,
  ce.product_category_name_english AS top_category_english
FROM SellerSales ss
LEFT JOIN SellerReviews sr ON ss.seller_id = sr.seller_id
LEFT JOIN SellerPackingTimes spt ON ss.seller_id = spt.seller_id
LEFT JOIN TopCategoryPerSeller tcs ON ss.seller_id = tcs.seller_id
LEFT JOIN CategoryNameEnglish ce ON tcs.product_category_name = ce.product_category_name
ORDER BY ss.total_sales DESC
LIMIT 1000;
