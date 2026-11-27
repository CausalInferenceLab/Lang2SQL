WITH employee_order_counts AS (
    SELECT o.employeeid,
           COUNT(*) AS total_orders,
           SUM(CASE WHEN DATE(o.shippeddate) >= DATE(o.requireddate) THEN 1 ELSE 0 END) AS late_orders
    FROM orders AS o
    JOIN employees AS e ON e.employeeid = o.employeeid
    GROUP BY o.employeeid
    HAVING COUNT(*) > 50
)
SELECT employeeid,
       late_orders,
       100.0 * late_orders / total_orders AS late_order_percentage
FROM employee_order_counts
ORDER BY late_order_percentage DESC, employeeid
LIMIT 3;
