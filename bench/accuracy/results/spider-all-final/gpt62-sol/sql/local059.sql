WITH product_totals AS (
    SELECT p.division, p.product_code, SUM(s.sold_quantity) AS total_quantity_sold
    FROM hardware_fact_sales_monthly AS s
    JOIN hardware_dim_product AS p ON p.product_code = s.product_code
    WHERE s.date >= '2021-01-01' AND s.date < '2022-01-01'
    GROUP BY p.division, p.product_code
), ranked_products AS (
    SELECT total_quantity_sold,
           ROW_NUMBER() OVER (
               PARTITION BY division
               ORDER BY total_quantity_sold DESC, product_code
           ) AS sales_rank
    FROM product_totals
)
SELECT AVG(total_quantity_sold) AS overall_average_quantity_sold
FROM ranked_products
WHERE sales_rank <= 3;
