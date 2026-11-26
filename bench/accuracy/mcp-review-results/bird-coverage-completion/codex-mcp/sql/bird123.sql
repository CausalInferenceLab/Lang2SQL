SELECT p.ViewCount, u.DisplayName AS LastEditorDisplayName
FROM main.posts AS p
JOIN main.users AS u ON u.Id = p.LastEditorUserId
WHERE p.Title = 'Computer game datasets'
LIMIT 1000
