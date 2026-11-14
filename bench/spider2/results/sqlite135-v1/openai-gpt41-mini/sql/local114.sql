WITH region_sales AS (
    SELECT
        wr.region_id,
        wr.region_name,
        COUNT(wo.order_id) AS number_of_orders,
        SUM(wo.total_amount) AS total_sales_amount
    FROM web_orders wo
    JOIN web_region wr ON wo.region_id = wr.region_id
    GROUP BY wr.region_id, wr.region_name
),
rep_sales AS (
    SELECT
        wr.region_id,
        wr.region_name,
        ws.sales_rep_id,
        ws.sales_rep_name,
        SUM(wo.total_amount) AS rep_total_sales
    FROM web_orders wo
    JOIN web_region wr ON wo.region_id = wr.region_id
    JOIN web_sales_reps ws ON wo.sales_rep_id = ws.sales_rep_id
    GROUP BY wr.region_id, wr.region_name, ws.sales_rep_id, ws.sales_rep_name
),
max_sales AS (
    SELECT
        region_id,
        MAX(rep_total_sales) AS max_rep_sales
    FROM rep_sales
    GROUP BY region_id
),
top_reps AS (
    SELECT
        rs.region_id,
        rs.region_name,
        rs.sales_rep_name,
        rs.rep_total_sales
    FROM rep_sales rs
    JOIN max_sales ms ON rs.region_id = ms.region_id AND rs.rep_total_sales = ms.max_rep_sales
)
SELECT
    r.region_id,
    r.region_name,
    r.number_of_orders,
    r.total_sales_amount,
    tr.sales_rep_name,
    tr.rep_total_sales AS sales_rep_amount
FROM region_sales r
JOIN top_reps tr ON r.region_id = tr.region_id
ORDER BY r.region_id, tr.sales_rep_name;
