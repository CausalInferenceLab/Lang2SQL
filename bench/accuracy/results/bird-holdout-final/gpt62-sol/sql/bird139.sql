SELECT COUNT(DISTINCT u.Id) AS user_count
FROM users AS u
JOIN badges AS b ON b.UserId = u.Id
WHERE u.Location LIKE '%India%'
  AND b.Name = 'Yearling'
  AND b.Date < '2014-01-01';
