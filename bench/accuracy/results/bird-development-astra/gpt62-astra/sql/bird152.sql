SELECT COUNT(DISTINCT c.CustomerID) AS customer_count
FROM customers AS c
WHERE c.Currency = 'EUR'
  AND EXISTS (
    SELECT 1 FROM yearmonth AS y
    WHERE y.CustomerID = c.CustomerID
      AND y."Date" >= '201201' AND y."Date" < '201301'
  )
  AND EXISTS (
    SELECT 1 FROM yearmonth AS y
    WHERE y.CustomerID = c.CustomerID
      AND y."Date" >= '201301' AND y."Date" < '201401'
  );
