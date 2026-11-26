WITH smoothed_sales AS (
    SELECT product_id, mth, qty,
           (AVG(qty) OVER (product_months ROWS BETWEEN 6 PRECEDING AND 5 FOLLOWING)
            + AVG(qty) OVER (product_months ROWS BETWEEN 5 PRECEDING AND 6 FOLLOWING)
           ) / 2.0 AS cma
    FROM main.monthly_sales
    WHERE mth >= '2016-01-01'
    WINDOW product_months AS (PARTITION BY product_id ORDER BY mth)
),
monthly_ratios AS (
    SELECT product_id, mth, qty / NULLIF(cma, 0) AS sales_to_cma
    FROM smoothed_sales
    WHERE mth >= '2016-07-01' AND mth < '2018-07-01'
)
SELECT p.id AS product_id, p.name AS product_name,
       MIN(r.sales_to_cma) AS lowest_monthly_ratio
FROM monthly_ratios AS r
JOIN main.products AS p ON p.id = r.product_id
WHERE r.mth >= '2017-01-01' AND r.mth < '2018-01-01'
GROUP BY p.id, p.name
HAVING COUNT(r.sales_to_cma) = 12 AND MIN(r.sales_to_cma) > 2
ORDER BY p.id
LIMIT 1000
