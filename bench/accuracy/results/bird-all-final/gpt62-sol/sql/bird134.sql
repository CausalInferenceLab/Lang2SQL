SELECT SUM(u.Reputation) AS combined_reputation
FROM (
    SELECT DISTINCT p.OwnerUserId
    FROM posts AS p
    WHERE p.LasActivityDate >= '2014-08-27'
      AND p.LasActivityDate < '2014-08-28'
      AND p.Tags LIKE '%<r>%'
) AS owners
JOIN users AS u ON u.Id = owners.OwnerUserId;
