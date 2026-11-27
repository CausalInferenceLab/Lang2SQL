WITH quarterly_consumption AS (
    SELECT CustomerID, SUM(Consumption) AS total_consumption
    FROM yearmonth
    WHERE Date IN ('201304', '201305', '201306')
      AND Consumption IS NOT NULL
    GROUP BY CustomerID
)
SELECT c.Currency,
       ROUND(AVG(q.total_consumption), 2) AS average_quarterly_consumption
FROM quarterly_consumption AS q
JOIN customers AS c ON c.CustomerID = q.CustomerID
WHERE c.Currency IN ('EUR', 'CZK')
GROUP BY c.Currency
ORDER BY c.Currency;
