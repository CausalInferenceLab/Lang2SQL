SELECT COUNT(DISTINCT p.ID) AS patient_count
FROM main.Patient AS p
JOIN main.Laboratory AS l ON l.ID = p.ID
WHERE l.TG >= 200
  AND p.Birthday <= DATE('2026-10-09', '-51 years')
LIMIT 1
