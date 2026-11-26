WITH japanese_printings AS (
    SELECT f.name AS japanese_name,
           c.type AS english_card_type,
           c.uuid,
           MAX(CASE WHEN c.watermark = 'boros' THEN 1 ELSE 0 END) AS has_boros_watermark
    FROM cards AS c
    JOIN foreign_data AS f ON f.uuid = c.uuid
    WHERE f.language = 'Japanese'
    GROUP BY f.name, c.type, c.uuid
)
SELECT p.japanese_name,
       p.english_card_type,
       COUNT(r.id) AS total_rulings
FROM japanese_printings AS p
LEFT JOIN rulings AS r ON r.uuid = p.uuid
GROUP BY p.japanese_name, p.english_card_type
HAVING MAX(p.has_boros_watermark) = 1
ORDER BY p.japanese_name, p.english_card_type;
