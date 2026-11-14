WITH toy_sales AS (
    SELECT
        DATE(o.order_purchase_timestamp) AS order_date,
        SUM(oi.price + oi.freight_value) AS sales
    FROM
        orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN products p ON oi.product_id = p.product_id
        JOIN product_category_name_translation t
            ON p.product_category_name = t.product_category_name
    WHERE
        t.product_category_name_english = 'toys'
        AND DATE(o.order_purchase_timestamp) BETWEEN '2017-01-01' AND '2018-08-29'
    GROUP BY
        DATE(o.order_purchase_timestamp)
),
-- Assign each date an index
dated_sales AS (
    SELECT
        order_date,
        sales,
        ROW_NUMBER() OVER (ORDER BY order_date) AS idx
    FROM toy_sales
),
-- Calculate regression coefficients
stats AS (
    SELECT
        COUNT(*) AS n,
        SUM(idx) AS sum_x,
        SUM(CAST(strftime('%s', order_date) AS INTEGER) / 86400.0) AS sum_date,
        SUM(sales) AS sum_y,
        SUM(idx * sales) AS sum_xy,
        SUM(idx * idx) AS sum_xx
    FROM dated_sales
),
-- Compute regression beta and alpha
regression AS (
    SELECT
        (n * sum_xy - sum_x * sum_y) * 1.0 / (n * sum_xx - sum_x * sum_x) AS beta,
        (sum_y * 1.0 / n) - ((n * sum_xy - sum_x * sum_y) * 1.0 / (n * sum_xx - sum_x * sum_x)) * (sum_x * 1.0 / n) AS alpha,
        (SELECT MAX(idx) FROM dated_sales) AS max_idx,
        (SELECT MAX(order_date) FROM dated_sales) AS last_date
    FROM stats
),
-- Predict daily sales for Dec 1~12, 2018 using regression
future_dates AS (
    -- We want all days so moving average centered on Dec 5-8 can be calculated
    SELECT
        DATE('2018-12-01', '+' || (n - 1) || ' days') AS pred_date,
        n
    FROM (
        SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
        UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
        UNION SELECT 11 UNION SELECT 12
    )
),
future_idx AS (
    -- Index: max_idx + days since last_date
    SELECT
        f.pred_date,
        r.alpha + r.beta * (r.max_idx + CAST(julianday(f.pred_date) - julianday(r.last_date) AS INTEGER)) AS pred_sales
    FROM
        future_dates f
        CROSS JOIN regression r
),
-- Calculate symmetric 5-day moving averages for Dec 5-8, 2018
moving_avg AS (
    SELECT
        f1.pred_date,
        AVG(f2.pred_sales) AS ma_5d
    FROM
        future_idx f1
        JOIN future_idx f2
            ON julianday(f2.pred_date) BETWEEN julianday(f1.pred_date, '-2 days') AND julianday(f1.pred_date, '+2 days')
    WHERE
        f1.pred_date BETWEEN '2018-12-05' AND '2018-12-08'
    GROUP BY
        f1.pred_date
)
SELECT SUM(ma_5d) AS sum_5day_moving_avg_dec5_to_dec8
FROM moving_avg;
