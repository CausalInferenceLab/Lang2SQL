SELECT COUNT(*) AS set_count
FROM main.sets AS s
WHERE s.block = 'Ice Age'
  AND s.code IN (
      SELECT t.setCode
      FROM main.set_translations AS t
      WHERE t.language = 'Italian'
        AND t.translation <> ''
  )
LIMIT 1
