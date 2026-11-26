SELECT COUNT(*) AS school_count
FROM satscores AS s
WHERE s.rtype = 'S'
  AND s.cname = 'Contra Costa'
  AND s.NumTstTakr > (
    SELECT t.NumTstTakr
    FROM satscores AS t
    WHERE t.cds = '07618040730051' AND t.rtype = 'S'
  );
