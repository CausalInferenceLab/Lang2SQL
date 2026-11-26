SELECT c.Segment AS client_segment,
       COUNT(DISTINCT ym.CustomerID) AS customers_who_consumed_fuel
FROM customers AS c
LEFT JOIN yearmonth AS ym
  ON ym.CustomerID = c.CustomerID
 AND ym.Date >= '201301'
 AND ym.Date < '201401'
 AND ym.Consumption IS NOT NULL
GROUP BY c.Segment;
