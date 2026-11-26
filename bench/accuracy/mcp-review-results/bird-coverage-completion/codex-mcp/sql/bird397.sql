SELECT p.Admission AS care_setting, COUNT(*) AS patient_count
FROM main.Patient AS p
WHERE p.Admission IN ('+', '-')
  AND p.ID IN (
      SELECT l.ID
      FROM main.Laboratory AS l
      WHERE CAST(l.RNP AS REAL) > 16
  )
GROUP BY p.Admission
ORDER BY p.Admission
LIMIT 2
