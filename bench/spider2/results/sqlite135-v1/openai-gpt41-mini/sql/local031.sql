WITH yearly_delivered_orders AS (
    SELECT
        strftime('%Y', order_purchase_timestamp) AS year,
        COUNT(order_id) AS delivered_orders
    FROM olist_orders
    WHERE order_status = 'delivered'
          AND strftime('%Y', order_purchase_timestamp) IN ('2016', '2017', '2018')
    GROUP BY year
),
lowest_year AS (
    SELECT year
    FROM yearly_delivered_orders
    ORDER BY delivered_orders ASC
    LIMIT 1
),
monthly_delivered_orders AS (
    SELECT
        strftime('%Y', order_purchase_timestamp) AS year,
        strftime('%m', order_purchase_timestamp) AS month,
        COUNT(order_id) AS delivered_orders
    FROM olist_orders
    WHERE order_status = 'delivered'
          AND strftime('%Y', order_purchase_timestamp) IN ('2016', '2017', '2018')
    GROUP BY year, month
)
SELECT MAX(monthly_delivered_orders.delivered_orders) AS highest_monthly_delivered_orders_in_lowest_year
FROM monthly_delivered_orders
JOIN lowest_year ON monthly_delivered_orders.year = lowest_year.year;
