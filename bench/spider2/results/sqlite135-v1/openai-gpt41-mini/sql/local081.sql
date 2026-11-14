WITH customer_spending_1998 AS (
    SELECT 
        o.customerid,
        SUM(od.unitprice * od.quantity) AS total_spent
    FROM orders o
    JOIN order_details od ON o.orderid = od.orderid
    WHERE strftime('%Y', o.orderdate) = '1998'
    GROUP BY o.customerid
),

customer_grouped AS (
    SELECT 
        cs.customerid,
        cs.total_spent,
        cgt.groupname
    FROM customer_spending_1998 cs
    JOIN customergroupthreshold cgt ON cs.total_spent >= cgt.rangebottom AND cs.total_spent < cgt.rangetop
),

customer_counts AS (
    SELECT 
        groupname,
        COUNT(*) AS customer_count
    FROM customer_grouped
    GROUP BY groupname
),

total_customers AS (
    SELECT COUNT(DISTINCT customerid) AS total_count
    FROM customer_spending_1998
)

SELECT 
    cc.groupname AS customer_group,
    cc.customer_count,
    ROUND(100.0 * cc.customer_count / tc.total_count, 2) AS percentage_of_total
FROM customer_counts cc
CROSS JOIN total_customers tc
ORDER BY cc.groupname;
