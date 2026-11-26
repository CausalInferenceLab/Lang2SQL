SELECT COUNT(*) AS patient_count
FROM main.Patient AS p
WHERE p.Birthday > '1960-01-01'
  AND NOT EXISTS (
      SELECT 1
      FROM main.Laboratory AS l
      WHERE l.ID = p.ID
        AND l.RNP <> 'negative'
        AND CAST(l.RNP AS REAL) > 10
  )
LIMIT 1
