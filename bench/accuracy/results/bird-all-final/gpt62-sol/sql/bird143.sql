SELECT COUNT(DISTINCT p.OwnerUserId)
     + COUNT(DISTINCT CASE WHEN p.OwnerUserId IS NULL THEN p.OwnerDisplayName END) AS user_count
FROM votes AS v
JOIN posts AS p ON p.Id = v.PostId
WHERE v.BountyAmount >= 30
  AND v.CreationDate >= '2010-01-01'
  AND v.CreationDate < '2011-01-01';
