WITH city_product_sales AS (
    SELECT c.cust_state_province_id,
           c.cust_city_id,
           s.prod_id,
           SUM(CASE WHEN t.calendar_year = 2019 THEN s.amount_sold ELSE 0.0 END) AS sales_2019,
           SUM(CASE WHEN t.calendar_year = 2020 THEN s.amount_sold ELSE 0.0 END) AS sales_2020
    FROM sales AS s
    JOIN customers AS c ON c.cust_id = s.cust_id
    JOIN countries AS co ON co.country_id = c.country_id
    JOIN times AS t ON t.time_id = s.time_id
    JOIN promotions AS pr ON pr.promo_id = s.promo_id
    WHERE co.country_iso_code = 'US'
      AND pr.promo_name = 'NO PROMOTION #'
      AND t.calendar_quarter_number = 4
      AND t.calendar_year IN (2019, 2020)
    GROUP BY c.cust_state_province_id, c.cust_city_id, s.prod_id
), qualifying_cities AS (
    SELECT cust_state_province_id, cust_city_id
    FROM city_product_sales
    GROUP BY cust_state_province_id, cust_city_id
    HAVING SUM(sales_2019) > 0
       AND SUM(sales_2020) >= 1.20 * SUM(sales_2019)
), product_sales AS (
    SELECT cps.prod_id,
           SUM(cps.sales_2019) AS sales_2019,
           SUM(cps.sales_2020) AS sales_2020
    FROM city_product_sales AS cps
    JOIN qualifying_cities AS qc
      ON qc.cust_state_province_id = cps.cust_state_province_id
     AND qc.cust_city_id = cps.cust_city_id
    GROUP BY cps.prod_id
), ranked_shares AS (
    SELECT prod_id,
           NTILE(5) OVER (ORDER BY sales_2019 + sales_2020 DESC, prod_id) AS sales_quintile,
           sales_2019 / NULLIF(SUM(sales_2019) OVER (), 0) AS share_q4_2019,
           sales_2020 / NULLIF(SUM(sales_2020) OVER (), 0) AS share_q4_2020
    FROM product_sales
)
SELECT p.prod_name,
       r.share_q4_2019,
       r.share_q4_2020,
       r.share_q4_2020 - r.share_q4_2019 AS share_change
FROM ranked_shares AS r
JOIN products AS p ON p.prod_id = r.prod_id
WHERE r.sales_quintile = 1
ORDER BY share_change DESC, r.prod_id;
