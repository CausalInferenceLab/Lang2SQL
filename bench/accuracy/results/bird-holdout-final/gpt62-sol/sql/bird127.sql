SELECT SUM(u.Reputation) AS total_reputation
FROM users AS u
WHERE u.Location LIKE '%United Kingdom%'
  AND EXISTS (
    SELECT 1
    FROM posts AS p
    WHERE p.OwnerUserId = u.Id
  );
