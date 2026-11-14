WITH prod_counts AS (
  SELECT
    segment,
    COUNT(DISTINCT hardware_fact_sales_monthly.product_code) AS unique_products,
    fiscal_year
  FROM hardware_fact_sales_monthly
  JOIN hardware_dim_product ON hardware_fact_sales_monthly.product_code = hardware_dim_product.product_code
  WHERE fiscal_year IN (2020, 2021)
  GROUP BY segment, fiscal_year
), 
pct_increase AS (
  SELECT
    p2020.segment,
    p2020.unique_products AS unique_products_2020,
    p2021.unique_products AS unique_products_2021,
    CASE 
      WHEN p2020.unique_products = 0 THEN NULL
      ELSE (CAST(p2021.unique_products AS FLOAT) - p2020.unique_products) / p2020.unique_products * 100
    END AS pct_increase
  FROM prod_counts p2020
  LEFT JOIN prod_counts p2021
    ON p2020.segment = p2021.segment
    AND p2021.fiscal_year = 2021
  WHERE p2020.fiscal_year = 2020
)
SELECT segment AS product_segment, unique_products_2020
FROM pct_increase
ORDER BY pct_increase DESC;
