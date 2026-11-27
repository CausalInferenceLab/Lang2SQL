SELECT COUNT(DISTINCT p.ID) AS patient_count
FROM Patient AS p
JOIN Laboratory AS l ON l.ID = p.ID
WHERE l.TG >= 200
  AND date(p.Birthday) <= date('2026-10-09', '-51 years');
