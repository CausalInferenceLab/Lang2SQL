SELECT SUM(s.totalSetSize) AS total_cards
FROM main.sets AS s
WHERE s.releaseDate >= '2017-01-01'
  AND s.releaseDate < '2018-01-01'
  AND s.code IN (
      SELECT t.setCode
      FROM main.set_translations AS t
      WHERE t.language <> 'English'
        AND TRIM(t.language) <> ''
        AND TRIM(t.translation) <> ''
  )
LIMIT 1
