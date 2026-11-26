SELECT
    ROUND(AVG(u.UpVotes), 2) AS average_upvotes_cast,
    ROUND(AVG(u.Age), 2) AS average_age
FROM main.users AS u
WHERE u.Id IN (
    SELECT p.OwnerUserId
    FROM main.posts AS p
    WHERE p.CreaionDate >= '2012-01-01'
      AND p.CreaionDate < '2013-01-01'
    GROUP BY p.OwnerUserId
    HAVING COUNT(*) > 10
)
LIMIT 1000
