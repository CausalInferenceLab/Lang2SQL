WITH customer_spending AS (
    SELECT o.customerid,
           COALESCE(SUM(od.unitprice * od.quantity), 0) AS total_spent
    FROM orders AS o
    LEFT JOIN order_details AS od ON od.orderid = o.orderid
    WHERE o.orderdate >= '1998-01-01'
      AND o.orderdate < '1999-01-01'
      AND o.customerid IS NOT NULL
    GROUP BY o.customerid
)
SELECT t.groupname AS spending_group,
       COUNT(s.customerid) AS customer_count,
       100.0 * COUNT(s.customerid) / (SELECT COUNT(*) FROM customer_spending) AS customer_percentage
FROM customergroupthreshold AS t
LEFT JOIN customer_spending AS s
    ON s.total_spent BETWEEN t.rangebottom AND t.rangetop
GROUP BY t.groupname, t.rangebottom, t.rangetop
ORDER BY t.rangebottom;
