SELECT c.originalType AS card_type, COUNT(*) AS card_count
FROM main.cards AS c
WHERE c.watermark = 'azorius'
  AND c.uuid IN (
      SELECT f.uuid
      FROM main.foreign_data AS f
      WHERE f.language IS NOT NULL AND f.language <> ''
  )
GROUP BY c.originalType
ORDER BY card_count DESC, card_type ASC
LIMIT 1000
