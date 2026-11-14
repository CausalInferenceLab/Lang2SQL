WITH sales_per_person_year AS (
    SELECT 
        soh.SalesPersonID,
        strftime('%Y', soh.OrderDate) AS Year,
        SUM(sod.LineTotal) AS TotalSales
    FROM salesorderheader soh
    JOIN salesorderdetail sod ON soh.SalesOrderID = sod.SalesOrderID
    WHERE soh.SalesPersonID IS NOT NULL
    GROUP BY soh.SalesPersonID, Year
),
quota_per_person_year AS (
    SELECT 
        SalesPersonID, 
        strftime('%Y', QuotaDate) AS Year, 
        SUM(SalesQuota) AS AnnualQuota
    FROM SalesPersonQuotaHistory
    GROUP BY SalesPersonID, Year
)
SELECT 
    s.SalesPersonID,
    s.Year,
    s.TotalSales,
    q.AnnualQuota,
    s.TotalSales - q.AnnualQuota AS Difference
FROM sales_per_person_year s
LEFT JOIN quota_per_person_year q 
    ON s.SalesPersonID = q.SalesPersonID AND s.Year = q.Year
ORDER BY s.SalesPersonID, s.Year;
