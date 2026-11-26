SELECT s.block,
       COUNT(*) AS translated_sets,
       SUM(s.baseSetSize) AS total_base_cards
FROM main.sets AS s
WHERE s.block IN ('Masques', 'Mirage')
  AND s.code IN (
      SELECT t.setCode
      FROM main.set_translations AS t
      WHERE TRIM(t.translation) <> ''
  )
GROUP BY s.block
ORDER BY s.block
LIMIT 2
