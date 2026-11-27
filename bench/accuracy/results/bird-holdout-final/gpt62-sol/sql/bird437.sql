SELECT COUNT(DISTINCT e.ID)
FROM Examination AS e
JOIN Laboratory AS l ON l.ID = e.ID
WHERE CAST(l.RNP AS REAL) > 0
  AND e.Thrombosis = 0;
