WITH ranked_cards AS (
    SELECT c.uuid, c.name, c.number,
           RANK() OVER (ORDER BY CAST(c.number AS INTEGER) DESC) AS number_rank
    FROM main.cards AS c
    WHERE c.watermark = 'abzan'
)
SELECT c.name,
       c.number AS collector_number,
       COUNT(f.id) AS foreign_language_printings
FROM ranked_cards AS c
LEFT JOIN main.foreign_data AS f ON f.uuid = c.uuid
WHERE c.number_rank = 1
GROUP BY c.uuid, c.name, c.number
ORDER BY c.name, c.uuid
LIMIT -1
