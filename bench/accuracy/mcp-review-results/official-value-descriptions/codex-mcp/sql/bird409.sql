SELECT COUNT(DISTINCT e.ID) AS patient_count
FROM main.Examination AS e
WHERE e."Examination Date" >= '1997-01-01'
  AND e."Examination Date" < '1998-01-01'
  AND e.Thrombosis IN (1, 2, 3)
LIMIT 1000
