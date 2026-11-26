SELECT u.Id
FROM main.users AS u
WHERE u.DisplayName = 'Michael'
  AND u.CreationDate IS NOT NULL
ORDER BY u.CreationDate DESC, u.Id ASC
LIMIT 1
