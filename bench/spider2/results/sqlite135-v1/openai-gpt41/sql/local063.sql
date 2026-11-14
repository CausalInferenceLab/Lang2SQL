WITH
-- Step 1: Filter sales for US, promo_id=999, Q4 2019 or Q4 2020
filtered_sales AS (
  SELECT s.*
  FROM sales s
  JOIN countries c ON s.country_id = c.country_id
  WHERE c.country_name = 'United States'
    AND s.promo_id = 999
    AND s.calendar_quarter_id IN (1772, 1776)
),
-- Step 2: Calculate total sales per city for each quarter
city_quarter_sales AS (
  SELECT city, calendar_quarter_id, SUM(sales_amt) AS total_sales
  FROM filtered_sales
  GROUP BY city, calendar_quarter_id
),
-- Step 3: Calculate percentage change from Q4 2019 to Q4 2020 for each city
city_sales_change AS (
  SELECT
    c19.city,
    c19.total_sales AS sales_2019,
    c20.total_sales AS sales_2020,
    (CAST(c20.total_sales AS REAL) - c19.total_sales) / NULLIF(c19.total_sales, 0) AS pct_change
  FROM
    (SELECT * FROM city_quarter_sales WHERE calendar_quarter_id = 1772) c19
    INNER JOIN
    (SELECT * FROM city_quarter_sales WHERE calendar_quarter_id = 1776) c20
    ON c19.city = c20.city
),
-- Step 4: Get cities with >=20% sales increase
qualified_cities AS (
  SELECT city
  FROM city_sales_change
  WHERE pct_change >= 0.20
),
-- Step 5: Restrict sales to qualified cities and relevant quarters
qualified_sales AS (
  SELECT s.*
  FROM filtered_sales s
  WHERE s.city IN (SELECT city FROM qualified_cities)
),
-- Step 6: Calculate total sales per product and quarter in the qualified scope
product_quarter_sales AS (
  SELECT product_id, calendar_quarter_id, SUM(sales_amt) AS prod_sales
  FROM qualified_sales
  GROUP BY product_id, calendar_quarter_id
),
-- Step 7: Total sales for each quarter in qualified scope
quarterly_totals AS (
  SELECT calendar_quarter_id, SUM(sales_amt) AS total_quarter_sales
  FROM qualified_sales
  GROUP BY calendar_quarter_id
),
-- Step 8: Calculate share of each product's sales of total for each quarter
product_share AS (
  SELECT
    p.product_id,
    p.calendar_quarter_id,
    p.prod_sales,
    q.total_quarter_sales,
    CASE WHEN q.total_quarter_sales > 0 THEN 1.0 * p.prod_sales / q.total_quarter_sales ELSE 0 END AS share
  FROM product_quarter_sales p
  JOIN quarterly_totals q ON p.calendar_quarter_id = q.calendar_quarter_id
),
-- Step 9: Combine Q4 2019 & Q4 2020 shares and compute change
product_share_change AS (
  SELECT
    p19.product_id,
    p19.share AS share_2019,
    p20.share AS share_2020,
    (p20.share - p19.share) AS share_point_change
  FROM
    (SELECT * FROM product_share WHERE calendar_quarter_id = 1772) p19
    JOIN
    (SELECT * FROM product_share WHERE calendar_quarter_id = 1776) p20
    ON p19.product_id = p20.product_id
),
-- Step 10: Compute total sales per product over both quarters
product_total_sales AS (
  SELECT product_id, SUM(prod_sales) AS total_prod_sales
  FROM product_quarter_sales
  GROUP BY product_id
),
-- Step 11: Determine sales cutoff for top 20%
sales_cutoff AS (
  SELECT
    PERCENTILE_DISC(0.80) WITHIN GROUP (ORDER BY total_prod_sales) AS cutoff
  FROM product_total_sales
),
-- Step 12: Select products in the top 20% of total sales
top_products AS (
  SELECT pts.product_id
  FROM product_total_sales pts, sales_cutoff c
  WHERE pts.total_prod_sales >= c.cutoff
),
-- Step 13: Get qualifying products and their share point changes
qualified_products AS (
  SELECT
    psc.product_id,
    psc.share_2019,
    psc.share_2020,
    psc.share_point_change
  FROM product_share_change psc
  WHERE psc.product_id IN (SELECT product_id FROM top_products)
)
SELECT
  qp.product_id,
  qp.share_2019,
  qp.share_2020,
  qp.share_point_change
FROM qualified_products qp
ORDER BY ABS(qp.share_point_change) ASC, qp.product_id
LIMIT 1;
