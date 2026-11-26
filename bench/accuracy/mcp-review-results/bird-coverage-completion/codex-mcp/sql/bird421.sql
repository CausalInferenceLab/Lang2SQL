SELECT COUNT(*) AS patient_count
FROM Patient AS p
WHERE p.Admission = '-'
  AND EXISTS (
      SELECT 1
      FROM Laboratory AS l
      WHERE l.ID = p.ID AND l.RA IN ('-', '+-')
  )
  AND NOT EXISTS (
      SELECT 1
      FROM Examination AS e
      WHERE e.ID = p.ID
        AND (e.KCT = '+' OR e.RVVT = '+' OR e.LAC = '+')
  )
LIMIT 1000
