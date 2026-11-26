SELECT ROUND(AVG(Consumption), 2) AS average_monthly_fuel_consumption
FROM main.yearmonth
WHERE CustomerID = 15621
  AND Date BETWEEN '201301' AND '201312'
  AND Consumption > 0
LIMIT 1
