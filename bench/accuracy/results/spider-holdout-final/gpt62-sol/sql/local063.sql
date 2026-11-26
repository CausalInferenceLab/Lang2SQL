WITH scoped_sales AS (
    SELECT c.cust_city_id, s.prod_id, t.calendar_quarter_id, s.amount_sold
    FROM sales AS s
    JOIN times AS t ON t.time_id = s.time_id
    JOIN customers AS c ON c.cust_id = s.cust_id
    JOIN countries AS co ON co.country_id = c.country_id
    WHERE co.country_iso_code = 'US'
      AND s.promo_id = 999
      AND t.calendar_quarter_id IN (1772, 1776)
),
city_totals AS (
    SELECT cust_city_id,
           SUM(CASE WHEN calendar_quarter_id = 1772 THEN amount_sold ELSE 0 END) AS sales_2019,
           SUM(CASE WHEN calendar_quarter_id = 1776 THEN amount_sold ELSE 0 END) AS sales_2020
    FROM scoped_sales
    GROUP BY cust_city_id
),
product_totals AS (
    SELECT s.prod_id,
           SUM(CASE WHEN s.calendar_quarter_id = 1772 THEN s.amount_sold ELSE 0 END) AS sales_2019,
           SUM(CASE WHEN s.calendar_quarter_id = 1776 THEN s.amount_sold ELSE 0 END) AS sales_2020
    FROM scoped_sales AS s
    JOIN city_totals AS c ON c.cust_city_id = s.cust_city_id
    WHERE 1.0 * c.sales_2020 / NULLIF(c.sales_2019, 0) >= 1.2
    GROUP BY s.prod_id
),
ranked_products AS (
    SELECT prod_id, sales_2019, sales_2020,
           SUM(sales_2019) OVER () AS total_2019,
           SUM(sales_2020) OVER () AS total_2020,
           RANK() OVER (ORDER BY sales_2019 + sales_2020 DESC) AS sales_rank,
           COUNT(*) OVER () AS product_count
    FROM product_totals
)
SELECT p.prod_name
FROM ranked_products AS r
JOIN products AS p ON p.prod_id = r.prod_id
WHERE r.sales_rank <= (r.product_count + 4) / 5
  AND r.total_2019 <> 0
  AND r.total_2020 <> 0
ORDER BY ABS(100.0 * (r.sales_2020 / r.total_2020 - r.sales_2019 / r.total_2019)),
         r.prod_id
LIMIT 1;
