WITH sales_data AS (
  SELECT 
    p.product_id,
    p.product_name,
    ms.year,
    ms.month,
    -- For continuous time indexing, assume months since Jan 2016 (month_index = (year-2016)*12 + (month-1) + 1)
    ((ms.year - 2016) * 12 + ms.month) AS month_index,
    ms.amount
  FROM
    monthly_sales ms
    JOIN products p ON ms.product_id = p.product_id
  WHERE
    (ms.year > 2015)
),
-- Compute CMA: For each product and month_index, average sales over two overlapping 12-month windows (5 before+6 after and 6 before+5 after)
cma_windows AS (
  SELECT
    s1.product_id,
    s1.product_name,
    s1.month_index,
    s1.amount,
    -- First window: 5 before + target + 6 after = 12 months
    (SELECT AVG(s2.amount) FROM sales_data s2 WHERE s2.product_id = s1.product_id AND s2.month_index BETWEEN s1.month_index - 5 AND s1.month_index + 6) AS win1_cma,
    -- Second window: 6 before + target + 5 after = 12 months
    (SELECT AVG(s2.amount) FROM sales_data s2 WHERE s2.product_id = s1.product_id AND s2.month_index BETWEEN s1.month_index - 6 AND s1.month_index + 5) AS win2_cma
  FROM sales_data s1
),
cma_final AS (
  SELECT
    product_id,
    product_name,
    month_index,
    amount,
    ( (win1_cma + win2_cma)/2.0 ) AS cma
  FROM cma_windows
),
-- Filter month_index for 2017 (Jan-Dec): Jan 2017 = 13, ..., Dec 2017 = 24
sales_vs_cma_2017 AS (
  SELECT
    product_id,
    product_name,
    month_index,
    amount,
    cma,
    CASE WHEN cma = 0 OR cma IS NULL THEN NULL ELSE amount * 1.0 / cma END AS sales_to_cma_ratio
  FROM cma_final
  WHERE month_index BETWEEN 13 AND 24
),
-- Find products where the ratio is always above 2 in 2017 and all 12 months exist
product_qualified AS (
  SELECT
    product_id,
    product_name,
    COUNT(*) AS months_present,
    SUM(CASE WHEN sales_to_cma_ratio > 2 THEN 1 ELSE 0 END) AS months_above_2
  FROM sales_vs_cma_2017
  GROUP BY product_id, product_name
)
SELECT
  product_id,
  product_name
FROM product_qualified
WHERE months_present = 12 AND months_above_2 = 12
ORDER BY product_id;
