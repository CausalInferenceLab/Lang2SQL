WITH type_counts AS (
    SELECT c.types AS card_type, COUNT(*) AS card_count
    FROM cards AS c
    WHERE c.watermark = 'boros'
      AND EXISTS (
          SELECT 1
          FROM foreign_data AS f
          WHERE f.uuid = c.uuid
            AND f.language COLLATE NOCASE <> 'English'
      )
    GROUP BY c.types
)
SELECT card_type, card_count
FROM type_counts
WHERE card_count = (SELECT MAX(card_count) FROM type_counts)
ORDER BY card_type;
