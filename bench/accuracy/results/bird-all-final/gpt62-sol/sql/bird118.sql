SELECT u.DisplayName
FROM posts AS p
JOIN users AS u ON u.Id = p.LastEditorUserId
WHERE p.Title = 'Examples for teaching: Correlation does not mean causation';
