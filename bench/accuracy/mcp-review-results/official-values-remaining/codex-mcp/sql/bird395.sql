WITH patient_counts AS (
    SELECT p.SEX, COUNT(l."Date") AS test_dates
    FROM main.Patient AS p
    LEFT JOIN main.Laboratory AS l ON l.ID = p.ID
    WHERE p.SEX IN ('F', 'M')
      AND p.ID NOT IN (
          SELECT e.ID
          FROM main.Examination AS e
          WHERE e.ID IS NOT NULL
      )
    GROUP BY p.ID, p.SEX
)
SELECT ROUND(AVG(CASE WHEN SEX = 'F' THEN test_dates END), 2) AS "F",
       ROUND(AVG(CASE WHEN SEX = 'M' THEN test_dates END), 2) AS "M"
FROM patient_counts
LIMIT 1000
