SELECT s.code, s.releaseDate
FROM main.sets AS s
WHERE s.releaseDate >= '2020-01-01'
  AND s.releaseDate < '2021-01-01'
  AND NOT EXISTS (
      SELECT 1
      FROM main.set_translations AS t
      WHERE t.setCode = s.code
        AND t.language = 'Spanish'
        AND t.translation IS NOT NULL
  )
ORDER BY s.releaseDate, s.code
LIMIT 1000
