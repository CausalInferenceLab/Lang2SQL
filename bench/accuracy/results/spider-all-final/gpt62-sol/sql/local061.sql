WITH monthly_sales AS (
    SELECT
        s.prod_id,
        co.country_name AS country,
        t.calendar_year AS sales_year,
        t.calendar_month_number AS month,
        SUM(s.amount_sold) AS sales_amount
    FROM sales AS s
    JOIN customers AS cu ON cu.cust_id = s.cust_id
    JOIN countries AS co ON co.country_id = cu.country_id
    JOIN times AS t ON t.time_id = s.time_id
    JOIN promotions AS p ON p.promo_id = s.promo_id
    JOIN channels AS ch ON ch.channel_id = s.channel_id
    WHERE co.country_name = 'France'
      AND p.promo_total_id = 1
      AND ch.channel_total_id = 1
      AND t.calendar_year BETWEEN 2019 AND 2021
    GROUP BY s.prod_id, co.country_name, t.calendar_year, t.calendar_month_number
), projections AS (
    SELECT
        target.country,
        target.month,
        y20.sales_amount +
            ((y20.sales_amount - y19.sales_amount) / NULLIF(y19.sales_amount, 0))
            * y20.sales_amount AS projected_sales
    FROM monthly_sales AS target
    LEFT JOIN monthly_sales AS y19
        ON y19.prod_id = target.prod_id
       AND y19.country = target.country
       AND y19.month = target.month
       AND y19.sales_year = 2019
    LEFT JOIN monthly_sales AS y20
        ON y20.prod_id = target.prod_id
       AND y20.country = target.country
       AND y20.month = target.month
       AND y20.sales_year = 2020
    WHERE target.sales_year = 2021
)
SELECT
    p.month,
    ROUND(AVG(p.projected_sales * COALESCE(c.to_us, 1.0)), 2) AS avg_projected_sales_usd
FROM projections AS p
LEFT JOIN currency AS c
    ON c.country = p.country
   AND c.year = 2021
   AND c.month = p.month
GROUP BY p.month
ORDER BY p.month;
