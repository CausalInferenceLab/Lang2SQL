SELECT c.type AS card_type, COUNT(*) AS card_count
FROM cards AS c
WHERE c.watermark = 'azorius'
  AND EXISTS (
    SELECT 1
    FROM foreign_data AS f
    WHERE f.uuid = c.uuid
  )
GROUP BY c.type
ORDER BY card_count DESC, card_type ASC;
