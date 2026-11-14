WITH monthly_sales_agg AS (
    SELECT
        strftime('%Y-%m', orders.ordered) AS year_month,
        orderlines.product_id,
        SUM(orderlines.amount) AS total_sales
    FROM orders
    JOIN orderlines ON orders.id = orderlines.order_id
    WHERE orders.ordered >= '2016-01-01' AND orders.ordered < '2018-01-01'
    GROUP BY year_month, orderlines.product_id
),
month_indexed AS (
    SELECT
        year_month,
        product_id,
        total_sales,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY year_month) AS month_idx
    FROM monthly_sales_agg
),
filtered_months AS (
    SELECT * FROM month_indexed
    WHERE month_idx BETWEEN 7 AND 30
),
cma_calculations AS (
    SELECT
        f1.year_month,
        f1.product_id,
        f1.month_idx,
        AVG(f2.total_sales) AS window1_avg,
        AVG(f3.total_sales) AS window2_avg
    FROM filtered_months f1
    LEFT JOIN filtered_months f2 ON f1.product_id = f2.product_id AND f2.month_idx BETWEEN f1.month_idx - 5 AND f1.month_idx + 6
    LEFT JOIN filtered_months f3 ON f1.product_id = f3.product_id AND f3.month_idx BETWEEN f1.month_idx - 6 AND f1.month_idx + 5
    GROUP BY f1.year_month, f1.product_id, f1.month_idx
),
cma_final AS (
    SELECT
        year_month,
        product_id,
        month_idx,
        (window1_avg + window2_avg) / 2.0 AS cma,
        total_sales
    FROM cma_calculations
    JOIN filtered_months USING (year_month, product_id, month_idx)
),
sales_ratio AS (
    SELECT
        year_month,
        product_id,
        month_idx,
        total_sales,
        cma,
        CASE WHEN cma = 0 THEN NULL ELSE total_sales / cma END AS sales_to_cma_ratio
    FROM cma_final
    WHERE cma IS NOT NULL
),
sales_2017 AS (
    SELECT *
    FROM sales_ratio
    WHERE year_month >= '2017-01' AND year_month <= '2017-12'
),
products_above_2_all_months AS (
    SELECT product_id
    FROM sales_2017
    WHERE sales_to_cma_ratio > 2
    GROUP BY product_id
    HAVING COUNT(*) = 12
)
SELECT products.id AS product_id, products.name AS product_name
FROM products_above_2_all_months
JOIN products ON products_above_2_all_months.product_id = products.id
ORDER BY products.id
LIMIT 1000;
