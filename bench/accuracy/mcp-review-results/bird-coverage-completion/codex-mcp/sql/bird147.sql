SELECT ROUND(
    100.0 * (COUNT(*) - COUNT(u.Id)) / NULLIF(COUNT(*), 0),
    2
) AS percentage
FROM posts AS p
LEFT JOIN users AS u
    ON u.Id = p.OwnerUserId AND u.Reputation >= 1000
WHERE p.CreaionDate >= '2010-01-01'
  AND p.CreaionDate < '2011-01-01'
  AND p.Score > 5
LIMIT 1
