SELECT ROUND(AVG(e."aCL IgM"), 2) AS average_acl_igm
FROM Examination AS e
WHERE e."aCL IgM" <> 0
  AND e.ID IN (
      SELECT p.ID
      FROM Patient AS p
      WHERE REPLACE(UPPER(p.Diagnosis), 'SJS SUSP', '') LIKE '%SJS%'
  );
