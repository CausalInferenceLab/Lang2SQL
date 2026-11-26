SELECT p.ViewCount,
       COALESCE(u.DisplayName, p.LastEditorDisplayName) AS LastEditorDisplayName
FROM posts AS p
LEFT JOIN users AS u ON u.Id = p.LastEditorUserId
WHERE p.Title = 'Computer game datasets';
