WITH region_sales AS (
    SELECT
        r.region_id,
        r.region_name,
        COUNT(o.order_id) AS num_orders,
        SUM(o.order_amount) AS total_sales_amount
    FROM web_region r
    LEFT JOIN web_orders o ON r.region_id = o.region_id
    GROUP BY r.region_id, r.region_name
),
rep_sales AS (
    SELECT
        r.region_id,
        rep.sales_rep_id,
        rep.sales_rep_name,
        SUM(o.order_amount) AS rep_total_sales
    FROM web_region r
    JOIN web_sales_reps rep ON r.region_id = rep.region_id
    LEFT JOIN web_orders o ON rep.sales_rep_id = o.sales_rep_id
    GROUP BY r.region_id, rep.sales_rep_id, rep.sales_rep_name
),
max_sales_per_region AS (
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
        rs.num_orders,
        rs.total_sales_amount,
        rps.sales_rep_name,
        rps.rep_total_sales
    FROM region_sales rs
    LEFT JOIN rep_sales rps ON rs.region_id = rps.region_id
    LEFT JOIN max_sales_per_region msr
        ON rps.region_id = msr.region_id AND rps.rep_total_sales = msr.max_rep_sales
    WHERE msr.max_rep_sales IS NOT NULL
)
SELECT
    region_id,
    region_name,
    num_orders,
    total_sales_amount,
    sales_rep_name,
    rep_total_sales AS sales_rep_total_sales
FROM top_reps
ORDER BY region_name, sales_rep_name
