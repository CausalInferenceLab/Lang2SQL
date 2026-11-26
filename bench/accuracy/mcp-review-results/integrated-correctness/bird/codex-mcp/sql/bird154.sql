WITH customers_with_fuel AS (
    SELECT DISTINCT ym.CustomerID
    FROM yearmonth AS ym
    WHERE ym.Date >= '201301'
      AND ym.Date < '201401'
      AND ym.Consumption IS NOT NULL
)
SELECT
    c.Segment,
    COUNT(f.CustomerID) AS CustomersConsumedFuel2013
FROM customers AS c
LEFT JOIN customers_with_fuel AS f ON f.CustomerID = c.CustomerID
GROUP BY c.Segment
LIMIT -1
