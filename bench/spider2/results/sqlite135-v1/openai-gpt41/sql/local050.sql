WITH
-- Filter sales for France, promo_total_id=1, channel_total_id=1, and for years 2019, 2020
filtered_sales AS (
    SELECT
        s.product_id,
        s.year,
        s.month,
        s.amount AS sales_amount,
        s.currency_code
    FROM sales s
    JOIN countries c ON s.country_id = c.country_id
    WHERE c.country_name = 'France'
      AND s.promo_total_id = 1
      AND s.channel_total_id = 1
      AND s.year IN (2019, 2020)
),
-- Aggregate sales by product, month, year
agg_sales AS (
    SELECT
        product_id,
        year,
        month,
        SUM(sales_amount) AS total_sales,
        currency_code
    FROM filtered_sales
    GROUP BY product_id, year, month, currency_code
),
-- Pivot 2019 and 2020 sales for projection
pivot_sales AS (
    SELECT
        a2019.product_id,
        a2019.month,
        COALESCE(a2019.total_sales, 0) AS sales_2019,
        COALESCE(a2020.total_sales, 0) AS sales_2020,
        COALESCE(a2020.currency_code, a2019.currency_code) AS currency_code
    FROM
        (SELECT * FROM agg_sales WHERE year = 2019) a2019
        JOIN (SELECT * FROM agg_sales WHERE year = 2020)
          a2020 ON a2019.product_id = a2020.product_id AND a2019.month = a2020.month
),
-- Calculate projected sales for 2021 for each product and month
projected_2021 AS (
    SELECT
        product_id,
        month,
        -- Projection formula as per documentation
        ROUND(
            CASE
                WHEN sales_2019 = 0 THEN sales_2020 -- fallback if 2019 is zero (avoid div by zero)
                ELSE (((sales_2020 - sales_2019) * 1.0) / sales_2019) * sales_2020 + sales_2020
            END,
            2
        ) AS projected_sales,
        currency_code
    FROM pivot_sales
),
-- Get monthly average projected sales (per month, average across products)
monthly_avg_projected AS (
    SELECT
        month,
        AVG(projected_sales) AS avg_projected_sales,
        currency_code
    FROM projected_2021
    GROUP BY month, currency_code
),
-- Attach USD conversion rates for each month/currency
monthly_with_fx AS (
    SELECT
        m.month,
        SUM(m.avg_projected_sales * COALESCE(c.usd_rate, 1)) AS avg_projected_usd
    FROM monthly_avg_projected m
    LEFT JOIN currency c
      ON m.currency_code = c.currency_code AND m.month = c.month AND c.year = 2021
    GROUP BY m.month
),
-- Get the list of 12 projected monthly averages in USD
monthly_usd_list AS (
    SELECT
        month,
        ROUND(avg_projected_usd,2) AS avg_projected_usd
    FROM monthly_with_fx
)
-- Calculate the median of the 12 values
SELECT
    AVG(avg_projected_usd) AS median_projected_usd
FROM (
    SELECT avg_projected_usd
    FROM monthly_usd_list
    ORDER BY avg_projected_usd
    LIMIT 2 - (SELECT COUNT(*) FROM monthly_usd_list) % 2 -- 1 if odd, 2 if even
    OFFSET (SELECT (COUNT(*) - 1) / 2 FROM monthly_usd_list)
);
