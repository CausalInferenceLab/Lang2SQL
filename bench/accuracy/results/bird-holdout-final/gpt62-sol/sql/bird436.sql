SELECT DISTINCT p.ID
FROM Patient AS p
JOIN Laboratory AS l ON l.ID = p.ID
WHERE p.Admission = '-'
  AND l.CPK < 250
  AND CAST(l.RNP AS REAL) > 4;
