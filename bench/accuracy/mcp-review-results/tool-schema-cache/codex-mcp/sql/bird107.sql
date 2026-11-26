SELECT
    f.name AS japanese_name,
    c.type AS english_card_type,
    COUNT(DISTINCT r.id) AS ruling_total
FROM main.foreign_data AS f
JOIN main.cards AS c ON c.uuid = f.uuid
LEFT JOIN main.rulings AS r ON r.uuid = c.uuid
WHERE f.language = 'Japanese'
GROUP BY f.name, c.type
HAVING MAX(CASE WHEN c.watermark = 'boros' THEN 1 ELSE 0 END) = 1
ORDER BY ruling_total DESC, f.name, c.type
LIMIT 1000
