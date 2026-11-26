WITH patient_lab_dates AS (
    SELECT p.ID, p.SEX, COUNT(DISTINCT l."Date") AS lab_date_count
    FROM Patient AS p
    LEFT JOIN Laboratory AS l ON l.ID = p.ID
    WHERE p.SEX IN ('F', 'M')
      AND NOT EXISTS (
          SELECT 1
          FROM Examination AS e
          WHERE e.ID = p.ID
      )
    GROUP BY p.ID, p.SEX
)
SELECT SEX, ROUND(AVG(lab_date_count), 2) AS avg_lab_test_dates
FROM patient_lab_dates
GROUP BY SEX
ORDER BY SEX;
