WITH country_sales AS (
    SELECT i.BillingCountry,
           SUM(CAST(ROUND(i.Total * 100) AS INTEGER)) AS sales_cents
    FROM main.invoices AS i
    WHERE i.BillingCountry IN (
        SELECT c.Country
        FROM main.customers AS c
        GROUP BY c.Country
        HAVING COUNT(*) > 4
    )
    GROUP BY i.BillingCountry
),
ranked_sales AS (
    SELECT sales_cents,
           ROW_NUMBER() OVER (ORDER BY sales_cents) AS position,
           COUNT(*) OVER () AS country_count
    FROM country_sales
)
SELECT AVG(sales_cents) / 100.0 AS median_total_sales
FROM ranked_sales
WHERE position BETWEEN (country_count + 1) / 2 AND (country_count + 2) / 2
LIMIT 1
