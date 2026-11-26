SELECT c.type AS card_type, COUNT(*) AS card_count
FROM cards AS c
WHERE c.watermark = 'azorius'
  AND c.uuid IN (
      SELECT f.uuid
      FROM foreign_data AS f
      WHERE f.language IS NOT NULL
  )
GROUP BY c.type
ORDER BY card_count DESC, card_type ASC
LIMIT -1
