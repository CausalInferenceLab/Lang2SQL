SELECT COUNT(DISTINCT p.ID)
FROM Patient AS p
WHERE DATE(p.Birthday) > '1960-01-01'
  AND NOT EXISTS (
    SELECT 1
    FROM Laboratory AS l
    WHERE l.ID = p.ID
      AND CAST(l.RNP AS REAL) > 10
  );
