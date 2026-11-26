SELECT
    p.ID,
    2026 - CAST(strftime('%Y', p.Birthday) AS INTEGER)
         - (strftime('%m-%d', p.Birthday) > '10-09') AS completed_age
FROM main.Patient AS p
WHERE p.ID IN (
    SELECT l.ID
    FROM main.Laboratory AS l
    WHERE l.HCT >= 52
    GROUP BY l.ID
    HAVING COUNT(*) >= 2
)
ORDER BY p.ID
LIMIT 2000
