WITH qualifying_customers AS (
    SELECT c.CustomerID
    FROM main.customers AS c
    JOIN main.yearmonth AS y ON y.CustomerID = c.CustomerID
    WHERE c.Segment = 'KAM'
      AND c.Currency = 'CZK'
      AND y.Date BETWEEN '201301' AND '201312'
    GROUP BY c.CustomerID
    HAVING SUM(y.Consumption) > 1000000
)
SELECT COUNT(*) AS customer_count
FROM qualifying_customers
LIMIT 1000
