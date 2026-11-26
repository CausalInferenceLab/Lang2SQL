SELECT ROUND(
    100.0 * COUNT(DISTINCT CASE WHEN p.Admission = '+' THEN p.ID END)
    / NULLIF(COUNT(DISTINCT p.ID), 0),
    2
) AS inpatient_percentage
FROM Patient AS p
WHERE p.Birthday >= '1930-01-01'
  AND p.Birthday < '1941-01-01'
  AND EXISTS (
      SELECT 1
      FROM Laboratory AS l
      WHERE l.ID = p.ID
  );
