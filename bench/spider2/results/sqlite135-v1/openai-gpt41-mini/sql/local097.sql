WITH YearCounts AS (
    SELECT 
        CAST(M.year AS INTEGER) AS start_year,
        (SELECT COUNT(*) FROM Movie WHERE CAST(year AS INTEGER) BETWEEN CAST(M.year AS INTEGER) AND CAST(M.year AS INTEGER) + 9) AS film_count
    FROM (SELECT DISTINCT year FROM Movie) M
)
SELECT start_year, film_count
FROM YearCounts
ORDER BY film_count DESC
LIMIT 1;
