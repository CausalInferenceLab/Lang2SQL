SELECT p.ID AS patient_id,
       p.SEX AS sex,
       CAST(strftime('%Y', '2026-10-09') AS INTEGER)
         - CAST(strftime('%Y', p.Birthday) AS INTEGER)
         - (strftime('%m-%d', p.Birthday) > strftime('%m-%d', '2026-10-09')) AS completed_age
FROM Patient AS p
WHERE EXISTS (
    SELECT 1
    FROM Laboratory AS l
    WHERE l.ID = p.ID
      AND l.GLU >= 180
      AND l."T-CHO" < 250
)
ORDER BY p.ID;
