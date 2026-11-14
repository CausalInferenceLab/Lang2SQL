WITH sales_us_promo AS (
    SELECT calendar_quarter_id, city, product_id, SUM(sales_amount) AS total_sales
    FROM sales
    WHERE country = 'United States' AND promo_id = 999
      AND calendar_quarter_id IN (1772, 1776)
    GROUP BY calendar_quarter_id, city, product_id
),
city_quarter_sales AS (
    SELECT calendar_quarter_id, city, SUM(total_sales) AS city_sales
    FROM sales_us_promo
    GROUP BY calendar_quarter_id, city
),
city_growth AS (
    SELECT a.city
    FROM city_quarter_sales a
    JOIN city_quarter_sales b ON a.city = b.city
    WHERE a.calendar_quarter_id = 1772 AND b.calendar_quarter_id = 1776
      AND (b.city_sales * 1.0 / a.city_sales) >= 1.2
),
filtered_sales AS (
    SELECT s.calendar_quarter_id, s.city, s.product_id, s.total_sales
    FROM sales_us_promo s
    WHERE s.city IN (SELECT city FROM city_growth)
),
product_quarter_sales AS (
    SELECT calendar_quarter_id, product_id, SUM(total_sales) AS product_sales
    FROM filtered_sales
    GROUP BY calendar_quarter_id, product_id
),
quarter_totals AS (
    SELECT calendar_quarter_id, SUM(product_sales) AS total_sales
    FROM product_quarter_sales
    GROUP BY calendar_quarter_id
),
product_shares AS (
    SELECT pqs.calendar_quarter_id, pqs.product_id, pqs.product_sales,
           qt.total_sales,
           (pqs.product_sales * 1.0 / qt.total_sales) AS sales_share
    FROM product_quarter_sales pqs
    JOIN quarter_totals qt ON pqs.calendar_quarter_id = qt.calendar_quarter_id
),
product_share_change AS (
    SELECT a.product_id,
           a.sales_share AS share_2019,
           b.sales_share AS share_2020,
           ABS(b.sales_share - a.sales_share) * 100 AS share_change_percentage_points,
           a.product_sales AS sales_2019
    FROM product_shares a
    JOIN product_shares b ON a.product_id = b.product_id
    WHERE a.calendar_quarter_id = 1772 AND b.calendar_quarter_id = 1776
),
ranked_products AS (
    SELECT product_id, share_change_percentage_points, sales_2019,
           NTILE(5) OVER (ORDER BY sales_2019 DESC) AS sales_rank_tile
    FROM product_share_change
)
SELECT product_id
FROM ranked_products
WHERE sales_rank_tile = 1
ORDER BY share_change_percentage_points
LIMIT 1;
