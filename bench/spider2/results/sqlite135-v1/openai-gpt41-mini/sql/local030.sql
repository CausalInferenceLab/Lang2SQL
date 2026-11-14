WITH city_payment_order AS (
    SELECT 
        c.customer_city,
        SUM(p.payment_value) AS total_payment,
        COUNT(DISTINCT o.order_id) AS total_delivered_orders
    FROM olist_customers c
    JOIN olist_orders o ON c.customer_id = o.customer_id
    JOIN olist_order_payments p ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_city
)
, lowest_5_cities AS (
    SELECT city_payment_order.*
    FROM city_payment_order
    ORDER BY total_payment ASC
    LIMIT 5
)
SELECT 
    AVG(total_payment) AS avg_total_payments,
    AVG(total_delivered_orders) AS avg_total_delivered_orders
FROM lowest_5_cities;
