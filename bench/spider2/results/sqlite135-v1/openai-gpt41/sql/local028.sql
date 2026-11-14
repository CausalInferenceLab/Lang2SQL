SELECT 
    strftime('%m', order_delivered_customer_date) AS month,
    SUM(CASE WHEN strftime('%Y', order_delivered_customer_date) = '2016' THEN 1 ELSE 0 END) AS orders_2016,
    SUM(CASE WHEN strftime('%Y', order_delivered_customer_date) = '2017' THEN 1 ELSE 0 END) AS orders_2017,
    SUM(CASE WHEN strftime('%Y', order_delivered_customer_date) = '2018' THEN 1 ELSE 0 END) AS orders_2018
FROM olist_orders
WHERE order_delivered_customer_date IS NOT NULL
    AND strftime('%Y', order_delivered_customer_date) IN ('2016', '2017', '2018')
GROUP BY month
ORDER BY month;
