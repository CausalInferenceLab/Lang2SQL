WITH moving_averages AS (
    SELECT product_id,
           mth,
           qty,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY mth) AS month_no,
           AVG(qty) OVER (
               PARTITION BY product_id ORDER BY mth
               ROWS BETWEEN 5 PRECEDING AND 6 FOLLOWING
           ) AS avg_left,
           AVG(qty) OVER (
               PARTITION BY product_id ORDER BY mth
               ROWS BETWEEN 6 PRECEDING AND 5 FOLLOWING
           ) AS avg_right
    FROM monthly_sales
    WHERE mth >= '2016-01-01'
), ratios AS (
    SELECT product_id,
           qty / NULLIF((avg_left + avg_right) / 2.0, 0) AS sales_to_cma
    FROM moving_averages
    WHERE month_no BETWEEN 7 AND 30
      AND mth >= '2017-01-01'
      AND mth < '2018-01-01'
), qualifying_products AS (
    SELECT product_id
    FROM ratios
    GROUP BY product_id
    HAVING COUNT(*) = 12
       AND COUNT(sales_to_cma) = 12
       AND MIN(sales_to_cma) > 2
)
SELECT p.name
FROM qualifying_products AS q
JOIN products AS p ON p.id = q.product_id
ORDER BY p.name;
