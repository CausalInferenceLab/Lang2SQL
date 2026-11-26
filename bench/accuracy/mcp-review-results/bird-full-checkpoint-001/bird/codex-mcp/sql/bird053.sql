SELECT ROUND(
    100.0 * COUNT(*) FILTER (WHERE c.uuid IN (
        SELECT f.uuid
        FROM foreign_data AS f
        WHERE f.language = 'French'
    )) / NULLIF(COUNT(*), 0),
    2
) AS french_translation_percentage
FROM cards AS c
WHERE c.isStorySpotlight = 1
LIMIT 1000
