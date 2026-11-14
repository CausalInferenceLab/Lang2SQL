WITH order_stats AS (
  SELECT
    o.employee_id,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN o.shipped_date >= o.required_date THEN 1 ELSE 0 END) AS late_orders
  FROM orders o
  GROUP BY o.employee_id
),
eligible_employees AS (
  SELECT
    employee_id,
    total_orders,
    late_orders,
    (CAST(late_orders AS FLOAT) / total_orders) AS late_order_pct
  FROM order_stats
  WHERE total_orders > 50
)
SELECT
  employee_id,
  late_orders,
  ROUND(late_order_pct * 100.0, 2) AS late_order_percentage
FROM eligible_employees
ORDER BY late_order_pct DESC
LIMIT 3;
