WITH order_payments AS (
    SELECT order_id, SUM(payment_value) AS total_payment
    FROM olist_order_payments
    GROUP BY order_id
), city_totals AS (
    SELECT c.customer_city,
           SUM(p.total_payment) AS total_payments,
           COUNT(DISTINCT o.order_id) AS delivered_order_count
    FROM olist_orders AS o
    JOIN olist_customers AS c ON c.customer_id = o.customer_id
    LEFT JOIN order_payments AS p ON p.order_id = o.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_city
), bottom_five AS (
    SELECT total_payments, delivered_order_count
    FROM city_totals
    ORDER BY total_payments ASC NULLS LAST, customer_city ASC
    LIMIT 5
)
SELECT AVG(total_payments) AS avg_total_payments,
       AVG(delivered_order_count) AS avg_delivered_order_count
FROM bottom_five;
