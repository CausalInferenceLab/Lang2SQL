WITH qualifying_patients AS (
    SELECT ID
    FROM Laboratory
    WHERE HCT >= 52
    GROUP BY ID
    HAVING COUNT(*) >= 2
)
SELECT p.ID,
       CAST(strftime('%Y', '2026-10-09') AS INTEGER)
       - CAST(strftime('%Y', p.Birthday) AS INTEGER)
       - (strftime('%m-%d', p.Birthday) > '10-09') AS age
FROM Patient AS p
JOIN qualifying_patients AS q ON q.ID = p.ID
ORDER BY p.ID;
