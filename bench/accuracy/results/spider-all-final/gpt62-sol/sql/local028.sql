WITH months(month) AS (
    VALUES ('01'), ('02'), ('03'), ('04'), ('05'), ('06'),
           ('07'), ('08'), ('09'), ('10'), ('11'), ('12')
), deliveries AS (
    SELECT strftime('%m', order_delivered_customer_date) AS month,
           strftime('%Y', order_delivered_customer_date) AS year
    FROM olist_orders
    WHERE order_status = 'delivered'
      AND order_delivered_customer_date >= '2016-01-01'
      AND order_delivered_customer_date < '2019-01-01'
)
SELECT m.month,
       COUNT(CASE WHEN d.year = '2016' THEN 1 END) AS "2016",
       COUNT(CASE WHEN d.year = '2017' THEN 1 END) AS "2017",
       COUNT(CASE WHEN d.year = '2018' THEN 1 END) AS "2018"
FROM months AS m
LEFT JOIN deliveries AS d ON d.month = m.month
GROUP BY m.month
ORDER BY m.month;
