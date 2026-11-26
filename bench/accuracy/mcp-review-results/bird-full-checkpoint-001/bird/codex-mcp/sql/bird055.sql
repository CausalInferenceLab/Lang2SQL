SELECT COUNT(DISTINCT c.uuid) AS printing_count
FROM cards AS c
JOIN legalities AS l ON l.uuid = c.uuid
WHERE c.borderColor = 'white'
  AND l.status = 'Banned'
  AND l.format IS NOT NULL
LIMIT 1
