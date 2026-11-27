WITH care_settings(admission) AS (
    VALUES ('+'), ('-')
), qualifying_patients AS (
    SELECT p.ID,
           p.Admission,
           2026 - CAST(strftime('%Y', p.Birthday) AS INTEGER)
                - (strftime('%m-%d', p.Birthday) > '10-09') AS completed_age
    FROM Patient AS p
    WHERE EXISTS (
        SELECT 1
        FROM Laboratory AS l
        WHERE l.ID = p.ID AND l.RBC < 3.5
    )
)
SELECT s.admission AS care_setting,
       COUNT(p.ID) AS patient_count,
       ROUND(AVG(p.completed_age), 1) AS average_completed_age
FROM care_settings AS s
LEFT JOIN qualifying_patients AS p ON p.Admission = s.admission
GROUP BY s.admission
ORDER BY s.admission;
