WITH segment_counts AS (
    SELECT
        p.segment,
        COUNT(DISTINCT CASE WHEN s.fiscal_year = 2020 THEN s.product_code END) AS unique_products_2020,
        COUNT(DISTINCT CASE WHEN s.fiscal_year = 2021 THEN s.product_code END) AS unique_products_2021
    FROM main.hardware_fact_sales_monthly AS s
    JOIN main.hardware_dim_product AS p ON p.product_code = s.product_code
    WHERE s.fiscal_year IN (2020, 2021)
    GROUP BY p.segment
)
SELECT segment, unique_products_2020
FROM segment_counts
ORDER BY
    100.0 * (unique_products_2021 - unique_products_2020) / NULLIF(unique_products_2020, 0) DESC,
    segment
LIMIT 1000
