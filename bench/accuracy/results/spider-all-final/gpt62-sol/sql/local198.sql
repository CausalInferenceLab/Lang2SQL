WITH eligible_countries AS (
    SELECT Country
    FROM customers
    GROUP BY Country
    HAVING COUNT(*) > 4
), country_sales AS (
    SELECT c.Country, ROUND(SUM(i.Total), 2) AS total_sales
    FROM eligible_countries AS e
    JOIN customers AS c ON c.Country = e.Country
    JOIN invoices AS i ON i.CustomerId = c.CustomerId
    GROUP BY c.Country
), ranked_sales AS (
    SELECT total_sales,
           ROW_NUMBER() OVER (ORDER BY total_sales) AS rn,
           COUNT(*) OVER () AS country_count
    FROM country_sales
)
SELECT ROUND(AVG(total_sales), 3) AS median_total_sales
FROM ranked_sales
WHERE rn IN ((country_count + 1) / 2, (country_count + 2) / 2);
