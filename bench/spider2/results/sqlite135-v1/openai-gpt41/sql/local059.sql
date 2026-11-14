WITH sales_2021 AS (
  SELECT 
    s.product_id,
    p.division,
    SUM(s.quantity_sold) AS total_quantity_sold
  FROM hardware_fact_sales_monthly s
  JOIN hardware_dim_product p ON s.product_id = p.product_id
  WHERE s.year = 2021
  GROUP BY s.product_id, p.division
),
top3_per_division AS (
  SELECT 
    division,
    product_id,
    total_quantity_sold,
    ROW_NUMBER() OVER (PARTITION BY division ORDER BY total_quantity_sold DESC) AS rn
  FROM sales_2021
),
top_products AS (
  SELECT 
    division,
    product_id,
    total_quantity_sold
  FROM top3_per_division
  WHERE rn <= 3
),
monthly_sales AS (
  SELECT 
    s.year,
    s.month,
    p.division,
    s.product_id,
    s.quantity_sold
  FROM hardware_fact_sales_monthly s
  JOIN hardware_dim_product p ON s.product_id = p.product_id
  WHERE s.year = 2021
    AND (p.division, s.product_id) IN (
      SELECT division, product_id FROM top_products
    )
)
SELECT 
  division,
  AVG(quantity_sold) AS avg_quantity_sold
FROM monthly_sales
GROUP BY division;
