WITH monthly_sales AS (
    SELECT s.prod_id,
           co.country_name,
           t.calendar_year AS sales_year,
           t.calendar_month_number AS sales_month,
           SUM(s.amount_sold) AS sales_amount
    FROM sales AS s
    JOIN times AS t ON t.time_id = s.time_id
    JOIN customers AS cu ON cu.cust_id = s.cust_id
    JOIN countries AS co ON co.country_id = cu.country_id
    JOIN promotions AS p ON p.promo_id = s.promo_id
    JOIN channels AS ch ON ch.channel_id = s.channel_id
    WHERE co.country_name = 'France'
      AND p.promo_total_id = 1
      AND ch.channel_total_id = 1
      AND t.calendar_year BETWEEN 2019 AND 2021
    GROUP BY s.prod_id, co.country_name, t.calendar_year, t.calendar_month_number
), monthly_averages AS (
    SELECT target.sales_month,
           ROUND(AVG(
               (sy20.sales_amount + (sy20.sales_amount - sy19.sales_amount)
                / NULLIF(sy19.sales_amount, 0) * sy20.sales_amount)
               * COALESCE(fx.to_us, 1.0)
           ), 2) AS avg_projected_sales_usd
    FROM monthly_sales AS target
    LEFT JOIN monthly_sales AS sy19
           ON sy19.prod_id = target.prod_id
          AND sy19.country_name = target.country_name
          AND sy19.sales_month = target.sales_month
          AND sy19.sales_year = 2019
    LEFT JOIN monthly_sales AS sy20
           ON sy20.prod_id = target.prod_id
          AND sy20.country_name = target.country_name
          AND sy20.sales_month = target.sales_month
          AND sy20.sales_year = 2020
    LEFT JOIN currency AS fx
           ON fx.country = target.country_name
          AND fx.year = target.sales_year
          AND fx.month = target.sales_month
    WHERE target.sales_year = 2021
    GROUP BY target.sales_month
), ranked AS (
    SELECT avg_projected_sales_usd,
           ROW_NUMBER() OVER (ORDER BY avg_projected_sales_usd) AS rn,
           COUNT(*) OVER () AS n
    FROM monthly_averages
    WHERE avg_projected_sales_usd IS NOT NULL
)
SELECT AVG(avg_projected_sales_usd) AS median_avg_monthly_projected_sales_usd
FROM ranked
WHERE rn IN ((n + 1) / 2, (n + 2) / 2);
