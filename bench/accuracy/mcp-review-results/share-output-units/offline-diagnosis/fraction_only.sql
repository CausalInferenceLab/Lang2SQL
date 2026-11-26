WITH city_product_sales AS (
    SELECT
        c.cust_city_id,
        s.prod_id,
        SUM(CASE WHEN s.time_id < '2020-01-01' THEN s.amount_sold ELSE 0 END) AS sales_2019,
        SUM(CASE WHEN s.time_id >= '2020-10-01' THEN s.amount_sold ELSE 0 END) AS sales_2020
    FROM main.sales AS s
    JOIN main.customers AS c ON c.cust_id = s.cust_id
    JOIN main.countries AS co ON co.country_id = c.country_id
    WHERE co.country_iso_code = 'US'
      AND s.promo_id = 999
      AND ((s.time_id >= '2019-10-01' AND s.time_id < '2020-01-01')
        OR (s.time_id >= '2020-10-01' AND s.time_id < '2021-01-01'))
    GROUP BY c.cust_city_id, s.prod_id
),
qualifying_cities AS (
    SELECT cust_city_id
    FROM city_product_sales
    GROUP BY cust_city_id
    HAVING SUM(sales_2019) > 0
       AND SUM(sales_2020) >= 1.2 * SUM(sales_2019)
),
product_sales AS (
    SELECT
        s.prod_id,
        SUM(s.sales_2019) AS sales_2019,
        SUM(s.sales_2020) AS sales_2020
    FROM city_product_sales AS s
    JOIN qualifying_cities AS c ON c.cust_city_id = s.cust_city_id
    GROUP BY s.prod_id
),
ranked_products AS (
    SELECT
        prod_id,
        NTILE(5) OVER (ORDER BY sales_2019 + sales_2020 DESC, prod_id) AS sales_quintile,
        1.0 * sales_2019 / NULLIF(SUM(sales_2019) OVER (), 0) AS q4_2019_share_pct,
        1.0 * sales_2020 / NULLIF(SUM(sales_2020) OVER (), 0) AS q4_2020_share_pct
    FROM product_sales
)
SELECT
    r.prod_id,
    p.prod_name,
    r.q4_2019_share_pct,
    r.q4_2020_share_pct,
    r.q4_2020_share_pct - r.q4_2019_share_pct AS share_change_pp
FROM ranked_products AS r
LEFT JOIN main.products AS p ON p.prod_id = r.prod_id
WHERE r.sales_quintile = 1
ORDER BY share_change_pp DESC, r.prod_id
LIMIT 1000
