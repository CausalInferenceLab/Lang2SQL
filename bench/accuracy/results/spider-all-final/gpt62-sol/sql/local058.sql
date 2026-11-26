WITH segment_counts AS (
    SELECT p.segment,
           COUNT(DISTINCT CASE WHEN s.fiscal_year = 2020 THEN s.product_code END) AS unique_products_2020,
           COUNT(DISTINCT CASE WHEN s.fiscal_year = 2021 THEN s.product_code END) AS unique_products_2021
    FROM hardware_dim_product AS p
    LEFT JOIN hardware_fact_sales_monthly AS s
        ON s.product_code = p.product_code
       AND s.fiscal_year IN (2020, 2021)
    GROUP BY p.segment
)
SELECT segment, unique_products_2020
FROM segment_counts
ORDER BY 100.0 * (unique_products_2021 - unique_products_2020)
         / NULLIF(unique_products_2020, 0) DESC,
         segment;
