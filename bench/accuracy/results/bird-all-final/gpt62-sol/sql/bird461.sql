SELECT COUNT(DISTINCT p.ID) AS patient_count
FROM Patient AS p
WHERE p.Admission = '-'
  AND EXISTS (
    SELECT 1
    FROM Laboratory AS l
    WHERE l.ID = p.ID
      AND l.RNP IN ('negative', '0')
  );
