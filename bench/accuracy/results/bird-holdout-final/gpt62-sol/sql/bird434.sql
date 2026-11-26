WITH first_tests AS (
    SELECT ID, MIN("Date") AS first_test_date
    FROM Laboratory
    GROUP BY ID
)
SELECT COUNT(DISTINCT p.ID) AS patient_count
FROM Patient AS p
JOIN first_tests AS f ON f.ID = p.ID
JOIN Laboratory AS l ON l.ID = f.ID AND l."Date" = f.first_test_date
WHERE l.GOT < 60
  AND CAST(l.RNP AS REAL) > 10;
