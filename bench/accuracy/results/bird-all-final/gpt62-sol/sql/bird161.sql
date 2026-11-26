SELECT COUNT(*) AS customer_count
FROM (
    SELECT c.CustomerID
    FROM customers AS c
    JOIN yearmonth AS ym ON ym.CustomerID = c.CustomerID
    WHERE c.Segment = 'KAM'
      AND c.Currency = 'CZK'
      AND ym.Date >= '201301'
      AND ym.Date < '201401'
    GROUP BY c.CustomerID
    HAVING SUM(ym.Consumption) > 1000000
) AS qualifying_customers;
