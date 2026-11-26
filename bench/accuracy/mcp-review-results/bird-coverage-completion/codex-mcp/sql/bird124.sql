SELECT c.Text, COALESCE(u.DisplayName, c.UserDisplayName) AS DisplayName
FROM posts AS p
JOIN comments AS c ON c.PostId = p.Id
LEFT JOIN users AS u ON u.Id = c.UserId
WHERE p.Title = 'Analysing wind data with R'
ORDER BY c.CreationDate DESC, c.Id DESC
LIMIT 10;
