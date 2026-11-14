WITH unique_products_2020 AS (
  SELECT
    p.segment AS segment,
    COUNT(DISTINCT p.product_id) AS unique_product_count_2020
  FROM hardware_dim_product p
  JOIN hardware_fact_sales_monthly s ON p.product_id = s.product_id
  WHERE strftime('%Y', s.sales_month) = '2020'
  GROUP BY p.segment
),
unique_products_2021 AS (
  SELECT
    p.segment AS segment,
    COUNT(DISTINCT p.product_id) AS unique_product_count_2021
  FROM hardware_dim_product p
  JOIN hardware_fact_sales_monthly s ON p.product_id = s.product_id
  WHERE strftime('%Y', s.sales_month) = '2021'
  GROUP BY p.segment
),
combined AS (
  SELECT
    u20.segment,
    u20.unique_product_count_2020,
    COALESCE(u21.unique_product_count_2021, 0) AS unique_product_count_2021,
    CASE 
      WHEN u20.unique_product_count_2020 = 0 THEN NULL
      ELSE (CAST(u21.unique_product_count_2021 AS FLOAT) - u20.unique_product_count_2020) / u20.unique_product_count_2020
    END AS pct_increase
  FROM unique_products_2020 u20
  LEFT JOIN unique_products_2021 u21 ON u20.segment = u21.segment
)
SELECT
  segment,
  unique_product_count_2020,
  pct_increase
FROM combined
ORDER BY pct_increase DESC
