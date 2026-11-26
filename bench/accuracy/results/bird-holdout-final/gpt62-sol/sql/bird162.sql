SELECT COUNT(DISTINCT c.CustomerID) AS sme_customer_count
FROM customers AS c
JOIN yearmonth AS ym ON ym.CustomerID = c.CustomerID
WHERE c.Segment = 'SME'
  AND ym.Date = '201302'
  AND ym.Consumption > 0;
