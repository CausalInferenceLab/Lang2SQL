WITH latest_proteinuria AS (
    SELECT ID, MAX("Date") AS test_date
    FROM Laboratory
    WHERE "U-PRO" IS NOT NULL
    GROUP BY ID
)
SELECT p.ID AS patient_id,
       p.SEX AS sex,
       l."Date" AS latest_proteinuria_test_date,
       l."U-PRO" AS proteinuria_level
FROM latest_proteinuria AS t
JOIN Laboratory AS l ON l.ID = t.ID AND l."Date" = t.test_date
JOIN Patient AS p ON p.ID = t.ID
WHERE l."U-PRO" IS NOT NULL
ORDER BY p.ID;
