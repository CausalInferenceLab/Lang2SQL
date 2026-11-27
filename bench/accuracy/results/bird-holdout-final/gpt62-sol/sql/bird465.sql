SELECT COUNT(DISTINCT p.ID) AS female_patient_count
FROM Patient AS p
JOIN Laboratory AS l ON l.ID = p.ID
WHERE p.SEX = 'F'
  AND l.WBC > 3.5
  AND l.WBC < 9.0
  AND (l.FG <= 150 OR l.FG >= 450)
  AND CAST(l.RNP AS REAL) > 15;
