WITH EmployeeOrderStats AS (
    SELECT 
        e.EmployeeID,
        COUNT(o.OrderID) AS TotalOrders,
        SUM(CASE WHEN o.ShippedDate >= o.RequiredDate THEN 1 ELSE 0 END) AS LateOrders
    FROM employees e
    JOIN orders o ON e.EmployeeID = o.EmployeeID
    GROUP BY e.EmployeeID
    HAVING TotalOrders > 50
)
SELECT 
    EmployeeID,
    LateOrders,
    ROUND(CAST(LateOrders AS FLOAT) / TotalOrders * 100, 2) AS LateOrderPercentage
FROM EmployeeOrderStats
ORDER BY LateOrderPercentage DESC
LIMIT 3;
