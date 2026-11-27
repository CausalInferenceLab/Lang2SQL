WITH customer_totals AS (
    SELECT c.CustomerID, c.Currency, SUM(y.Consumption) AS total_consumption
    FROM customers AS c
    JOIN yearmonth AS y ON y.CustomerID = c.CustomerID
    WHERE y."Date" >= '201201' AND y."Date" < '201301'
      AND c.Currency IN ('CZK', 'EUR')
    GROUP BY c.CustomerID, c.Currency
)
SELECT CAST(ROUND(
    AVG(CASE WHEN Currency = 'CZK' THEN total_consumption END)
    - AVG(CASE WHEN Currency = 'EUR' THEN total_consumption END)
) AS INTEGER) AS consumption_difference
FROM customer_totals;
