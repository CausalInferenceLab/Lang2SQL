SELECT COUNT(DISTINCT p.ID) AS patient_count
FROM Patient AS p
JOIN Laboratory AS l ON l.ID = p.ID
WHERE p.Description IS NULL
  AND l.CRP LIKE '%+%'
LIMIT 1
