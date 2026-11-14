WITH usa_sales AS (
  SELECT
    cu.cust_city AS city,
    s.prod_id AS product_id,
    t.calendar_year AS year,
    t.calendar_quarter_number AS quarter,
    SUM(s.amount_sold) AS total_sales
  FROM sales s
  JOIN times t ON s.time_id = t.time_id
  JOIN customers cu ON s.cust_id = cu.cust_id
  WHERE cu.country_id = (SELECT country_id FROM countries WHERE country_name = 'United States')
    AND t.calendar_year IN (2019, 2020)
    AND t.calendar_quarter_number = 4
    AND s.promo_id = 0
  GROUP BY cu.cust_city, s.prod_id, t.calendar_year, t.calendar_quarter_number
),
city_growth AS (
  SELECT
    city,
    SUM(CASE WHEN year = 2019 THEN total_sales ELSE 0 END) AS sales_2019,
    SUM(CASE WHEN year = 2020 THEN total_sales ELSE 0 END) AS sales_2020,
    CASE WHEN SUM(CASE WHEN year = 2019 THEN total_sales ELSE 0 END) > 0 THEN
      SUM(CASE WHEN year = 2020 THEN total_sales ELSE 0 END) * 1.0 / SUM(CASE WHEN year = 2019 THEN total_sales ELSE 0 END)
      ELSE NULL
    END AS sales_growth
  FROM usa_sales
  GROUP BY city
  HAVING sales_growth >= 1.2
),
filtered_sales AS (
  SELECT s.city, s.product_id, s.year, s.quarter, s.total_sales
  FROM usa_sales s
  JOIN city_growth cg ON s.city = cg.city
  WHERE s.year IN (2019, 2020) AND s.quarter = 4
),
ranked_products AS (
  SELECT
    city,
    product_id,
    SUM(total_sales) AS total_sales_all_quarters,
    RANK() OVER (PARTITION BY city ORDER BY SUM(total_sales) DESC) AS sales_rank,
    COUNT(*) OVER (PARTITION BY city) AS product_count
  FROM filtered_sales
  GROUP BY city, product_id
),
filtered_top_products AS (
  SELECT city, product_id, total_sales_all_quarters
  FROM ranked_products
  WHERE sales_rank <= CEIL(product_count * 0.2)
),
product_annual_sales AS (
  SELECT
    f.city, f.product_id, f.year, f.quarter, f.total_sales
  FROM filtered_sales f
  JOIN filtered_top_products ftp ON f.city = ftp.city AND f.product_id = ftp.product_id
),
product_city_year_total AS (
  SELECT
    city, year, SUM(total_sales) AS city_year_total_sales
  FROM usa_sales
  WHERE city IN (SELECT city FROM city_growth)
    AND year IN (2019, 2020)
  GROUP BY city, year
),
product_share AS (
  SELECT
    p.city,
    p.product_id,
    p.year,
    p.total_sales,
    (p.total_sales * 1.0) / c.city_year_total_sales AS sales_share
  FROM product_annual_sales p
  JOIN product_city_year_total c ON p.city = c.city AND p.year = c.year
  WHERE p.quarter = 4
),
share_changes AS (
  SELECT
    p2019.city,
    p2019.product_id,
    p2019.sales_share AS share_2019,
    p2020.sales_share AS share_2020,
    (p2020.sales_share - p2019.sales_share) AS share_change
  FROM product_share p2019
  JOIN product_share p2020 ON p2019.city = p2020.city AND p2019.product_id = p2020.product_id AND p2020.year = 2020
  WHERE p2019.year = 2019
)

SELECT city, product_id, share_2019, share_2020, share_change
FROM share_changes
ORDER BY share_change DESC
;
