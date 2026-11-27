SELECT COUNT(DISTINCT f.CDSCode) AS school_count
FROM frpm AS f
WHERE f."Enrollment (K-12)" > 500
  AND TRIM(f."High Grade") IN ('1', '2', '3', '4', '5', '6', '7', '8')
  AND NULLIF(TRIM(f."School Name"), '') IS NOT NULL
  AND f."School Code" <> '0000000'
  AND NOT EXISTS (
      SELECT 1
      FROM satscores AS s
      WHERE s.cds = f.CDSCode
        AND s.rtype = 'S'
        AND s.NumTstTakr > 0
  );
