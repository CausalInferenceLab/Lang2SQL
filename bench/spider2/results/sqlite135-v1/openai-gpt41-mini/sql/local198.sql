WITH country_sales AS (
  SELECT c.Country,
         COUNT(c.CustomerId) AS num_customers,
         SUM(ii.UnitPrice * ii.Quantity) AS total_sales
  FROM customers c
  JOIN invoices i ON c.CustomerId = i.CustomerId
  JOIN invoice_items ii ON i.InvoiceId = ii.InvoiceId
  GROUP BY c.Country
  HAVING COUNT(c.CustomerId) > 4
),
ranked_sales AS (
  SELECT total_sales,
         ROW_NUMBER() OVER (ORDER BY total_sales) AS row_num,
         COUNT(*) OVER () AS total_rows
  FROM country_sales
)
SELECT AVG(total_sales) AS median_total_sales
FROM ranked_sales
WHERE row_num IN ((total_rows + 1) / 2, (total_rows + 2) / 2);
