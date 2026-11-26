WITH customer_totals AS (
    SELECT c.CustomerID, c.Currency, SUM(y.Consumption) AS annual_consumption
    FROM main.customers AS c
    JOIN main.yearmonth AS y ON y.CustomerID = c.CustomerID
    WHERE y.Date BETWEEN '201201' AND '201212'
      AND c.Currency IN ('CZK', 'EUR')
    GROUP BY c.CustomerID, c.Currency
)
SELECT CAST(ROUND(
    AVG(CASE WHEN Currency = 'CZK' THEN annual_consumption END)
    - AVG(CASE WHEN Currency = 'EUR' THEN annual_consumption END)
) AS INTEGER) AS consumption_difference
FROM customer_totals
LIMIT 1
