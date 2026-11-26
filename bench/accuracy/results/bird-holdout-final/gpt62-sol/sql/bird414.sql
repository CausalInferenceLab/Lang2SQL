WITH lab_panels AS (
    SELECT ID, LDH, TRIM("U-PRO") AS protein
    FROM Laboratory
)
SELECT p.ID AS patient_id,
       2026 - CAST(STRFTIME('%Y', p.Birthday) AS INTEGER)
            - (STRFTIME('%m-%d', p.Birthday) > '10-09') AS age
FROM Patient AS p
WHERE EXISTS (
    SELECT 1
    FROM lab_panels AS l
    WHERE l.ID = p.ID
      AND l.LDH > 600
      AND l.LDH < 800
      AND CAST(CASE
          WHEN l.protein LIKE '>=%' THEN SUBSTR(l.protein, 3)
          WHEN l.protein LIKE '≥%' THEN SUBSTR(l.protein, 2)
          WHEN LOWER(l.protein) LIKE 'at least %' THEN SUBSTR(l.protein, 10)
          ELSE l.protein
      END AS REAL) >= 300
)
ORDER BY p.ID;
