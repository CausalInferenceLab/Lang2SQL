SELECT
    j.name AS japanese_name,
    c.type AS english_card_type,
    COUNT(DISTINCT r.id) AS total_rulings
FROM foreign_data AS j
JOIN cards AS c ON c.uuid = j.uuid
LEFT JOIN rulings AS r ON r.uuid = c.uuid
WHERE j.language = 'Japanese'
GROUP BY j.name, c.type
HAVING MAX(CASE WHEN c.watermark = 'boros' THEN 1 ELSE 0 END) = 1
ORDER BY total_rulings DESC, j.name, c.type
LIMIT 1000
