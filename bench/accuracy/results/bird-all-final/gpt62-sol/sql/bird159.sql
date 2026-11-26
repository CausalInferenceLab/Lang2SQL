SELECT ROUND(AVG(Consumption), 2) AS average_monthly_fuel_consumption
FROM yearmonth
WHERE CustomerID = 15621
  AND Date >= '201301'
  AND Date < '201401'
  AND Consumption > 0;
