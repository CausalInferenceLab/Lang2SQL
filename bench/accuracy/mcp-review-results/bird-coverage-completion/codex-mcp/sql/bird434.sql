WITH first_lab_dates AS (
    SELECT ID, MIN("Date") AS first_date
    FROM main.Laboratory
    GROUP BY ID
)
SELECT COUNT(*) AS patient_count
FROM first_lab_dates AS f
JOIN main.Laboratory AS l
  ON l.ID = f.ID AND l."Date" = f.first_date
WHERE l.GOT < 60
  AND CAST(l.RNP AS REAL) > 10
LIMIT 1
