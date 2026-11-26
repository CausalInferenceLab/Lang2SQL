SELECT COUNT(*) AS printing_count
FROM cards AS c
WHERE SUBSTR(c.originalReleaseDate, 1, 4) = '2014'
  AND c.setCode IN (
      SELECT s.code
      FROM sets AS s
      WHERE TRIM(s.mtgoCode) <> ''
  )
LIMIT 1
