WITH rep_sales AS (
    SELECT sr.region_id,
           sr.id AS rep_id,
           sr.name AS rep_name,
           COUNT(o.id) AS order_count,
           SUM(o.total_amt_usd) AS rep_sales_amount
    FROM web_sales_reps AS sr
    JOIN web_accounts AS a ON a.sales_rep_id = sr.id
    JOIN web_orders AS o ON o.account_id = a.id
    GROUP BY sr.region_id, sr.id, sr.name
), region_sales AS (
    SELECT region_id,
           SUM(order_count) AS order_count,
           SUM(rep_sales_amount) AS total_sales_amount,
           MAX(rep_sales_amount) AS highest_rep_sales_amount
    FROM rep_sales
    GROUP BY region_id
)
SELECT r.name AS region,
       COALESCE(rs.order_count, 0) AS order_count,
       rs.total_sales_amount,
       s.rep_name AS top_sales_rep,
       s.rep_sales_amount AS top_sales_rep_amount
FROM web_region AS r
LEFT JOIN region_sales AS rs ON rs.region_id = r.id
LEFT JOIN rep_sales AS s ON s.region_id = r.id
                        AND s.rep_sales_amount = rs.highest_rep_sales_amount
ORDER BY r.name, s.rep_name, s.rep_id;
