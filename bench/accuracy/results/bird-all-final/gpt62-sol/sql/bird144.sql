WITH posters AS (
    SELECT DISTINCT OwnerUserId
    FROM posts
    WHERE CreaionDate >= '2010-01-01'
      AND CreaionDate < '2011-01-01'
)
SELECT ROUND(AVG(u.Reputation), 2) AS average_reputation
FROM posters AS p
JOIN users AS u ON u.Id = p.OwnerUserId
WHERE LOWER(TRIM(u.Location)) = 'india'
   OR LOWER(TRIM(u.Location)) LIKE '%, india';
