SELECT COUNT(DISTINCT a.account_id)
FROM account AS a
WHERE a.district_id = 1
  AND a.date < '1996-01-01'
  AND EXISTS (
    SELECT 1
    FROM loan AS l
    WHERE l.account_id = a.account_id
      AND l.status IN ('C', 'D')
  );
