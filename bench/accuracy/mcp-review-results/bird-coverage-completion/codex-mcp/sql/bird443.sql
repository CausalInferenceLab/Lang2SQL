SELECT COUNT(*) AS patient_count
FROM main.Patient AS p
WHERE p.Admission = '-'
  AND (
      SELECT l.IGG
      FROM main.Laboratory AS l
      WHERE l.ID = p.ID AND l.IGG IS NOT NULL
      ORDER BY l."Date" DESC
      LIMIT 1
  ) >= 2000
LIMIT 1
