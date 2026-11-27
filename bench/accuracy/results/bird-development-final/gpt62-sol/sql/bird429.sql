WITH lab_results AS (
    SELECT ID, FG, TRIM("U-PRO") AS protein
    FROM Laboratory
)
SELECT COUNT(DISTINCT p.ID) AS eligible_patient_count
FROM Patient AS p
JOIN lab_results AS l ON l.ID = p.ID
WHERE DATE(p.Birthday) > '1980-01-01'
  AND (
      l.FG < 150 OR l.FG > 450
      OR l.protein IN ('3', '4', '3+', '4+')
      OR CAST(CASE WHEN l.protein LIKE '>=%' THEN SUBSTR(l.protein, 3)
                   ELSE l.protein END AS REAL) >= 300
  );
