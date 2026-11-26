SELECT COUNT(DISTINCT p.ID) AS patient_count
FROM Patient AS p
JOIN Laboratory AS l ON l.ID = p.ID
WHERE p.SEX = 'F'
  AND CAST(l.DNA AS REAL) < 8;
