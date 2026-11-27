SELECT ROUND(100.0 * AVG(CASE WHEN p.Score > 50 THEN 1 ELSE 0 END), 2) AS percentage
FROM posts AS p
WHERE p.OwnerUserId = (
    SELECT u.Id
    FROM users AS u
    ORDER BY u.Reputation DESC, u.Id
    LIMIT 1
);
