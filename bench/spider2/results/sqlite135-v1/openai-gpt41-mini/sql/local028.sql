WITH delivered_orders AS (
  SELECT order_id, strftime('%Y', order_delivered_customer_date) AS year, strftime('%m', order_delivered_customer_date) AS month
  FROM olist_orders
  WHERE order_status = 'delivered'
    AND order_delivered_customer_date IS NOT NULL
    AND strftime('%Y', order_delivered_customer_date) IN ('2016', '2017', '2018')
)
SELECT month,
       COUNT(CASE WHEN year = '2016' THEN order_id END) AS "2016",
       COUNT(CASE WHEN year = '2017' THEN order_id END) AS "2017",
       COUNT(CASE WHEN year = '2018' THEN order_id END) AS "2018"
FROM delivered_orders
GROUP BY month
ORDER BY month;
