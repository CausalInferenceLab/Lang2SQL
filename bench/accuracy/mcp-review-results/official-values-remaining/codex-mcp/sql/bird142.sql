SELECT Id
FROM main.users
WHERE DisplayName = 'Michael'
  AND CreationDate IS NOT NULL
ORDER BY CreationDate DESC, Id ASC
LIMIT 1
