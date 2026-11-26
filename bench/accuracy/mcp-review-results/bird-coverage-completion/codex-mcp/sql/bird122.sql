WITH posts_2010 AS (
    SELECT p.Id, p.OwnerUserId, p.FavoriteCount
    FROM main.posts AS p
    WHERE p.CreaionDate >= '2010-01-01'
      AND p.CreaionDate < '2011-01-01'
)
SELECT p.Id AS PostId, u.DisplayName
FROM posts_2010 AS p
LEFT JOIN main.users AS u ON u.Id = p.OwnerUserId
WHERE p.FavoriteCount = (SELECT MAX(FavoriteCount) FROM posts_2010)
LIMIT 1000
