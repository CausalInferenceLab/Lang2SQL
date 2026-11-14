WITH sales_2021 AS (
  SELECT p.division, f.product_code, SUM(f.sold_quantity) AS total_quantity_sold
  FROM hardware_fact_sales_monthly f
  JOIN hardware_dim_product p ON f.product_code = p.product_code
  WHERE f.fiscal_year = 2021
  GROUP BY p.division, f.product_code
), ranked_products AS (
  SELECT division, product_code, total_quantity_sold,
         RANK() OVER (PARTITION BY division ORDER BY total_quantity_sold DESC) AS rank_within_division
  FROM sales_2021
), top3_products AS (
  SELECT division, product_code, total_quantity_sold
  FROM ranked_products
  WHERE rank_within_division <= 3
), overall_avg_quantity AS (
  SELECT AVG(total_quantity_sold) AS avg_quantity_sold
  FROM top3_products
)
SELECT avg_quantity_sold FROM overall_avg_quantity;
