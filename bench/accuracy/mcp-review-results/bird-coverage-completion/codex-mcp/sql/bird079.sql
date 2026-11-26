SELECT SUM(s.totalSetSize) AS total_cards
FROM sets AS s
WHERE s.releaseDate >= '2017-01-01'
  AND s.releaseDate < '2018-01-01'
  AND s.code IN (
      SELECT t.setCode
      FROM set_translations AS t
      WHERE t.language <> 'English'
        AND TRIM(t.translation) <> ''
  )
LIMIT 1
