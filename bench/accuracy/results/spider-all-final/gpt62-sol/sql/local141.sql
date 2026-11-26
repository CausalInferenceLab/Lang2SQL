WITH annual_sales AS (
    SELECT salespersonid AS salesperson_id,
           CAST(strftime('%Y', orderdate) AS INTEGER) AS sales_year,
           SUM(totaldue) AS total_sales
    FROM salesorderheader
    WHERE salespersonid IS NOT NULL
    GROUP BY salespersonid, CAST(strftime('%Y', orderdate) AS INTEGER)
), annual_quotas AS (
    SELECT BusinessEntityID AS salesperson_id,
           CAST(strftime('%Y', QuotaDate) AS INTEGER) AS sales_year,
           SUM(SalesQuota) AS annual_quota
    FROM SalesPersonQuotaHistory
    GROUP BY BusinessEntityID, CAST(strftime('%Y', QuotaDate) AS INTEGER)
)
SELECT s.salesperson_id,
       s.sales_year,
       s.total_sales - q.annual_quota AS sales_minus_quota
FROM annual_sales AS s
LEFT JOIN annual_quotas AS q
    ON q.salesperson_id = s.salesperson_id
   AND q.sales_year = s.sales_year
ORDER BY s.salesperson_id, s.sales_year;
