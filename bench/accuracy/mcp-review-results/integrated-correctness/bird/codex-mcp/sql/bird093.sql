WITH type_counts AS (
    SELECT c.types AS card_type, COUNT(*) AS card_count
    FROM main.cards AS c
    WHERE c.watermark = 'boros'
      AND c.uuid IN (
          SELECT f.uuid
          FROM main.foreign_data AS f
          WHERE f.language <> 'English'
      )
    GROUP BY c.types
)
SELECT card_type, card_count
FROM type_counts
WHERE card_count = (SELECT MAX(card_count) FROM type_counts)
ORDER BY card_type
LIMIT -1
