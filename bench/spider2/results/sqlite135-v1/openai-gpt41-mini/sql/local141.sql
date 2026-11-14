WITH SalesPerYear AS (
    SELECT
        soh.SalesPersonID,
        strftime('%Y', soh.OrderDate) AS Year,
        SUM(sod.LineTotal) AS TotalSales
    FROM salesorderheader soh
    JOIN salesorderdetail sod ON soh.SalesOrderID = sod.SalesOrderID
    WHERE soh.SalesPersonID IS NOT NULL
    GROUP BY soh.SalesPersonID, Year
),
QuotaPerYear AS (
    SELECT
        Spqh.BusinessEntityID AS SalesPersonID,
        strftime('%Y', Spqh.QuotaDate) AS Year,
        MAX(Spqh.SalesQuota) AS AnnualQuota
    FROM SalesPersonQuotaHistory Spqh
    GROUP BY Spqh.BusinessEntityID, Year
)
SELECT 
    spy.Year,
    spyh.SalesPersonID,
    sp.LastName || ', ' || sp.FirstName AS SalesPersonName,
    spy.TotalSales,
    spqh.AnnualQuota,
    (spy.TotalSales - spqh.AnnualQuota) AS Difference
FROM SalesPerYear spy
JOIN QuotaPerYear spqh ON spy.SalesPersonID = spqh.SalesPersonID AND spy.Year = spqh.Year
JOIN salesperson spyh ON spyh.BusinessEntityID = spy.SalesPersonID
JOIN salesperson sp ON sp.BusinessEntityID = spy.SalesPersonID
ORDER BY spyh.SalesPersonID, spy.Year;
