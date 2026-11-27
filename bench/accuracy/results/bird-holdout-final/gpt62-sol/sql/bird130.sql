WITH first_posts AS (
    SELECT OwnerUserId, MIN(CreaionDate) AS first_post_at
    FROM posts
    GROUP BY OwnerUserId
)
SELECT COUNT(DISTINCT u.Id) AS user_count
FROM first_posts AS f
JOIN users AS u ON u.Id = f.OwnerUserId
WHERE f.first_post_at >= '2010-01-01'
  AND f.first_post_at < '2011-01-01'
  AND (
      LOWER(TRIM(u.Location)) = 'india'
      OR REPLACE(LOWER(TRIM(u.Location)), ', ', ',') LIKE '%,india'
  );
