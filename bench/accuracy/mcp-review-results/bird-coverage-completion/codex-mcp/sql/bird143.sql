SELECT COUNT(DISTINCT COALESCE(p.OwnerUserId, NULLIF(p.OwnerDisplayName, ''))) AS user_count
FROM main.votes AS v
JOIN main.posts AS p ON p.Id = v.PostId
WHERE v.BountyAmount >= 30
  AND v.CreationDate >= '2010-01-01'
  AND v.CreationDate < '2011-01-01'
LIMIT 1
