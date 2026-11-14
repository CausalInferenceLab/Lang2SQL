WITH filtered_sales AS (
  SELECT
    s.product_id,
    s.country_id,
    s.channel_id,
    s.promo_id,
    s.time_id,
    s.amount as sales_amount
  FROM sales s
  JOIN promotions p ON s.promo_id = p.promotion_id
  JOIN channels c ON s.channel_id = c.channel_id
  JOIN countries ct ON s.country_id = ct.country_id
  WHERE ct.country = 'France'
    AND c.channel_total_id = 1
    AND p.promo_total_id = 1
),
sales_with_time AS (
  SELECT
    fs.product_id,
    fs.country_id,
    fs.channel_id,
    fs.promo_id,
    t.year,
    t.month,
    fs.sales_amount
  FROM filtered_sales fs
  JOIN times t ON fs.time_id = t.time_id
  WHERE t.year IN (2019, 2020)
),
sales_agg AS (
  SELECT
    product_id,
    month,
    SUM(CASE WHEN year = 2019 THEN sales_amount ELSE 0 END) AS sales_2019,
    SUM(CASE WHEN year = 2020 THEN sales_amount ELSE 0 END) AS sales_2020
  FROM sales_with_time
  GROUP BY product_id, month
  HAVING sales_2019 > 0 -- To avoid division by zero, consider only products/months with sales in 2019
),
projected_2021 AS (
  SELECT
    product_id,
    month,
    -- (((2020-2019)/2019)*2020) + 2020 = (2020/2019 - 1)*2020 + 2020 = (2020/2019)*2020
    ((CAST(sales_2020 AS FLOAT) - sales_2019) / sales_2019) * sales_2020 + sales_2020 AS projected_2021_sales
  FROM sales_agg
),
currency_rates_2021 AS (
  SELECT
    t.month,
    c1.currency_code AS local_currency,
    c2.currency_code AS usd_currency,
    cr1.conversion_rate_to_usd
  FROM times t
  JOIN countries ct ON ct.country = 'France'
  JOIN currency c1 ON ct.currency_id = c1.currency_id
  LEFT JOIN currency c2 ON c2.currency_code = 'USD'
  LEFT JOIN (
    SELECT
      year,
      month,
      currency_id,
      conversion_rate_to_usd
    FROM currency
    WHERE year = 2021
      AND currency_code = 'EUR' -- Assume France uses EUR, adjust if schema differs
  ) cr1 ON cr1.year = 2021 AND cr1.month = t.month AND cr1.currency_id = c1.currency_id
  GROUP BY t.month
),
projected_2021_usd AS (
  SELECT
    p.month,
    CASE
      WHEN cr.conversion_rate_to_usd IS NOT NULL AND cr.conversion_rate_to_usd > 0
        THEN projected_2021_sales * cr.conversion_rate_to_usd
      ELSE projected_2021_sales
    END AS projected_2021_sales_usd
  FROM projected_2021 p
  LEFT JOIN currency_rates_2021 cr ON p.month = cr.month
)
SELECT
  month,
  ROUND(AVG(projected_2021_sales_usd), 2) AS avg_projected_2021_sales_usd
FROM projected_2021_usd
GROUP BY month
ORDER BY month
;
