SELECT ROUND(100.0 * AVG(CASE WHEN NOT EXISTS (
    SELECT 1
    FROM users AS u
    WHERE u.Id = p.OwnerUserId
      AND u.Reputation >= 1000
) THEN 1 ELSE 0 END), 2) AS percentage
FROM posts AS p
WHERE p.CreaionDate >= '2010-01-01'
  AND p.CreaionDate < '2011-01-01'
  AND p.Score > 5;
