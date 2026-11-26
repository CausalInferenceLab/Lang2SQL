WITH first_posts AS (
    SELECT p.OwnerUserId, MIN(p.CreaionDate) AS first_post_date
    FROM main.posts AS p
    GROUP BY p.OwnerUserId
)
SELECT COUNT(*) AS user_count
FROM first_posts AS f
JOIN main.users AS u ON u.Id = f.OwnerUserId
WHERE f.first_post_date >= '2010-01-01'
  AND f.first_post_date < '2011-01-01'
  AND (' ' || REPLACE(TRIM(u.Location), ',', ' ')) LIKE '% India'
LIMIT 1
