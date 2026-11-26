WITH active_customers AS (
    SELECT CustomerID
    FROM yearmonth
    WHERE Date BETWEEN '201301' AND '201311'
    GROUP BY CustomerID
    HAVING COUNT(DISTINCT Date) = 11
)
SELECT c.Currency, COUNT(DISTINCT a.CustomerID) AS customer_count
FROM customers AS c
LEFT JOIN active_customers AS a ON a.CustomerID = c.CustomerID
GROUP BY c.Currency;
