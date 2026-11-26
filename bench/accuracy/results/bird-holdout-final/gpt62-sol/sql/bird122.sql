SELECT p.Id AS PostId, u.DisplayName AS OwnerDisplayName
FROM posts AS p
LEFT JOIN users AS u ON u.Id = p.OwnerUserId
WHERE p.CreaionDate >= '2010-01-01'
  AND p.CreaionDate < '2011-01-01'
ORDER BY p.FavoriteCount DESC, p.Id ASC
LIMIT 1;
