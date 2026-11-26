SELECT
    p.ID,
    p.SEX,
    2026 - CAST(strftime('%Y', p.Birthday) AS INTEGER)
         - (strftime('%m-%d', p.Birthday) > '10-09') AS completed_age
FROM Patient AS p
WHERE p.ID IN (
    SELECT l.ID
    FROM Laboratory AS l
    WHERE l.GLU >= 180
      AND l."T-CHO" < 250
)
ORDER BY p.ID
LIMIT -1
