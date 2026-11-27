SELECT COUNT(DISTINCT p.ID) AS patient_count
FROM Patient AS p
WHERE p.Description IS NULL
  AND EXISTS (
    SELECT 1
    FROM Laboratory AS l
    WHERE l.ID = p.ID
      AND l.CRP LIKE '%+%'
  );
