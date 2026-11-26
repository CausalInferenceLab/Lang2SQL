WITH qualifying_customers AS (
    SELECT CustomerID
    FROM yearmonth
    WHERE Date >= '201201' AND Date < '201301'
    GROUP BY CustomerID
    HAVING SUM(Consumption) < 30000
)
SELECT c.Segment, COUNT(q.CustomerID) AS CustomerCount
FROM customers AS c
LEFT JOIN qualifying_customers AS q ON q.CustomerID = c.CustomerID
GROUP BY c.Segment
ORDER BY c.Segment;
