WITH active_customers AS (
    SELECT DISTINCT y.CustomerID
    FROM main.yearmonth AS y
    WHERE y.Date >= '201301'
      AND y.Date < '201401'
      AND y.Consumption > 0
)
SELECT c.Segment, COUNT(a.CustomerID) AS customer_count
FROM main.customers AS c
LEFT JOIN active_customers AS a ON a.CustomerID = c.CustomerID
GROUP BY c.Segment
LIMIT 1000
