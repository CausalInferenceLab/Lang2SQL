SELECT ROUND(AVG(NULLIF(e."aCL IgM", 0)), 2) AS avg_acl_igm
FROM Examination AS e
WHERE EXISTS (
    SELECT 1
    FROM Patient AS p
    WHERE p.ID = e.ID
      AND UPPER(p.Diagnosis) LIKE '%SJS%'
      AND UPPER(p.Diagnosis) NOT LIKE '%SJS SUSP%'
);
