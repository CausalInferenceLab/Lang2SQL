WITH qualifying_customers AS (
    SELECT y.CustomerID
    FROM main.yearmonth AS y
    WHERE y.Date >= '201201' AND y.Date < '201301'
    GROUP BY y.CustomerID
    HAVING SUM(y.Consumption) < 30000
)
SELECT c.Segment, COUNT(q.CustomerID) AS customer_count
FROM main.customers AS c
LEFT JOIN qualifying_customers AS q ON q.CustomerID = c.CustomerID
GROUP BY c.Segment
ORDER BY c.Segment
LIMIT 1000
