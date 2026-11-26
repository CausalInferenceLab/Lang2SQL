SELECT ROUND(AVG(NULLIF(e."aCL IgM", 0)), 2) AS average_anticardiolipin_igm
FROM Examination AS e
JOIN Patient AS p ON p.ID = e.ID
WHERE UPPER(p.Diagnosis) LIKE '%SJS%'
  AND UPPER(p.Diagnosis) NOT LIKE '%SJS SUSP%';
