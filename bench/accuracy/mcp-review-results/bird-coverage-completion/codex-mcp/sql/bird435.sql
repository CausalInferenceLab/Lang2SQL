WITH latest_test AS (
    SELECT l.ID, MAX(l."Date") AS test_date
    FROM main.Laboratory AS l
    WHERE l."U-PRO" IS NOT NULL
    GROUP BY l.ID
)
SELECT p.ID, p.SEX, t.test_date, l."U-PRO" AS proteinuria_level
FROM latest_test AS t
JOIN main.Patient AS p ON p.ID = t.ID
JOIN main.Laboratory AS l ON l.ID = t.ID AND l."Date" = t.test_date
ORDER BY p.ID
LIMIT -1
