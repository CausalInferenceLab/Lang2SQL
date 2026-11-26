WITH monthly AS (
    SELECT strftime('%Y', order_delivered_customer_date) AS delivery_year,
           strftime('%Y-%m', order_delivered_customer_date) AS delivery_month,
           COUNT(*) AS delivered_orders
    FROM olist_orders
    WHERE order_status = 'delivered'
      AND order_delivered_customer_date >= '2016-01-01'
      AND order_delivered_customer_date < '2019-01-01'
    GROUP BY 1, 2
), annual AS (
    SELECT delivery_year, SUM(delivered_orders) AS delivered_orders
    FROM monthly
    GROUP BY delivery_year
)
SELECT MAX(m.delivered_orders) AS highest_monthly_delivered_orders
FROM monthly AS m
JOIN annual AS a ON a.delivery_year = m.delivery_year
WHERE a.delivered_orders = (SELECT MIN(delivered_orders) FROM annual);
