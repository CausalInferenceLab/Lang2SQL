SELECT SUM(u.Reputation) AS combined_reputation
FROM main.users AS u
WHERE u.Id IN (
    SELECT p.OwnerUserId
    FROM main.posts AS p
    WHERE p.Tags LIKE '%<r>%'
      AND p.LasActivityDate >= '2014-08-27'
      AND p.LasActivityDate < '2014-08-28'
)
LIMIT 1000
