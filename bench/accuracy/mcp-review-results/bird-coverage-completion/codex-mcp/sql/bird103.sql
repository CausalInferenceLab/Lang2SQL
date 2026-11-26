SELECT s.code, s.releaseDate
FROM sets AS s
WHERE s.releaseDate >= '2020-01-01'
  AND s.releaseDate < '2021-01-01'
  AND NOT EXISTS (
      SELECT 1
      FROM set_translations AS t
      WHERE t.setCode = s.code
        AND t.language = 'Spanish'
        AND t.translation IS NOT NULL
        AND t.translation <> ''
  )
ORDER BY s.releaseDate, s.code
LIMIT -1
