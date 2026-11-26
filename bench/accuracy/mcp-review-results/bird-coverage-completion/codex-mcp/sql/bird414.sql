WITH review_date(as_of) AS (VALUES ('2026-10-09'))
SELECT p.ID,
       CAST(strftime('%Y', d.as_of) AS INTEGER)
       - CAST(strftime('%Y', p.Birthday) AS INTEGER)
       - (strftime('%m-%d', d.as_of) < strftime('%m-%d', p.Birthday)) AS completed_age
FROM main.Patient AS p
CROSS JOIN review_date AS d
WHERE p.ID IN (
    SELECT l.ID
    FROM main.Laboratory AS l
    WHERE l.LDH > 600
      AND l.LDH < 800
      AND CAST(
          CASE
              WHEN l."U-PRO" LIKE '>=%' THEN substr(l."U-PRO", 3)
              WHEN l."U-PRO" GLOB '[0-9]*' THEN l."U-PRO"
          END AS REAL
      ) >= 300
)
ORDER BY p.ID
LIMIT 1000
