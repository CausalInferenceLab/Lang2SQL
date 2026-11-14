WITH orders_1998 AS (
  SELECT o.orderid, o.customerid
  FROM orders o
  WHERE strftime('%Y', o.orderdate) = '1998'
),
customer_spending AS (
  SELECT
    o.customerid,
    SUM(od.unitprice * od.quantity) AS total_spent
  FROM orders_1998 o
  JOIN order_details od ON o.orderid = od.orderid
  GROUP BY o.customerid
),
spending_group AS (
  SELECT
    cs.customerid,
    cs.total_spent,
    cgt.group_name
  FROM customer_spending cs
  JOIN customergroupthreshold cgt
    ON cs.total_spent >= cgt.min_threshold
   AND (cs.total_spent < cgt.max_threshold OR cgt.max_threshold IS NULL)
)
SELECT
  sg.group_name,
  COUNT(*) AS customer_count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customer_spending), 2) AS percent_of_customers
FROM spending_group sg
GROUP BY sg.group_name
ORDER BY customer_count DESC, sg.group_name;
