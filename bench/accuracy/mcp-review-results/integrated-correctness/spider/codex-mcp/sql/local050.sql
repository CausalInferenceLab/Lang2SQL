WITH product_month_sales AS (
    SELECT s.prod_id,
           t.calendar_year AS sales_year,
           t.calendar_month_number AS month,
           SUM(s.amount_sold) AS amount
    FROM main.sales AS s
    JOIN main.customers AS cu ON cu.cust_id = s.cust_id
    JOIN main.countries AS co ON co.country_id = cu.country_id
    JOIN main.times AS t ON t.time_id = s.time_id
    JOIN main.promotions AS p ON p.promo_id = s.promo_id
    JOIN main.channels AS ch ON ch.channel_id = s.channel_id
    WHERE co.country_name = 'France'
      AND p.promo_total_id = 1
      AND ch.channel_total_id = 1
      AND t.calendar_year IN (2019, 2020, 2021)
    GROUP BY s.prod_id, t.calendar_year, t.calendar_month_number
),
monthly_averages AS (
    SELECT target.month,
           ROUND(AVG(
               (
                   (recent.amount - prior.amount) / NULLIF(prior.amount, 0)
                   * recent.amount + recent.amount
               ) * COALESCE(fx.to_us, 1.0)
           ), 2) AS average_projected_sales_usd
    FROM product_month_sales AS target
    LEFT JOIN product_month_sales AS prior
      ON prior.prod_id = target.prod_id
     AND prior.month = target.month
     AND prior.sales_year = 2019
    LEFT JOIN product_month_sales AS recent
      ON recent.prod_id = target.prod_id
     AND recent.month = target.month
     AND recent.sales_year = 2020
    LEFT JOIN main.currency AS fx
      ON fx.country = 'France'
     AND fx.year = target.sales_year
     AND fx.month = target.month
    WHERE target.sales_year = 2021
    GROUP BY target.month
),
ranked_months AS (
    SELECT average_projected_sales_usd,
           ROW_NUMBER() OVER (ORDER BY average_projected_sales_usd) AS month_rank,
           COUNT(*) OVER () AS month_count
    FROM monthly_averages
    WHERE average_projected_sales_usd IS NOT NULL
)
SELECT AVG(average_projected_sales_usd) AS median_average_monthly_projected_sales_usd
FROM ranked_months
WHERE month_rank IN ((month_count + 1) / 2, (month_count + 2) / 2)
LIMIT 1
