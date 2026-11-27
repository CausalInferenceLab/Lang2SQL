WITH active_users AS (
    SELECT OwnerUserId
    FROM posts
    WHERE CreaionDate >= '2012-01-01'
      AND CreaionDate < '2013-01-01'
    GROUP BY OwnerUserId
    HAVING COUNT(*) > 10
)
SELECT ROUND(AVG(u.UpVotes), 2) AS average_upvotes_cast,
       ROUND(AVG(u.Age), 2) AS average_age
FROM active_users AS a
JOIN users AS u ON u.Id = a.OwnerUserId;
