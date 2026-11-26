SELECT DISTINCT c.name
FROM main.cards AS c
WHERE c.isPromo = 1
  AND c.otherFaceIds <> ''
  AND substr(c.originalReleaseDate, 1, 4) = '2019'
ORDER BY c.name
LIMIT 1000
