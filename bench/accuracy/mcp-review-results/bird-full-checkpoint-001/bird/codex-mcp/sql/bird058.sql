SELECT ROUND(
    100.0 * COUNT(CASE WHEN c.uuid IN (
        SELECT f.uuid
        FROM main.foreign_data AS f
        WHERE f.language = 'French'
    ) THEN 1 END) / NULLIF(COUNT(*), 0),
    2
) AS french_translation_percentage
FROM main.cards AS c
WHERE c.power IS NULL OR c.power LIKE '%*%'
LIMIT 1
