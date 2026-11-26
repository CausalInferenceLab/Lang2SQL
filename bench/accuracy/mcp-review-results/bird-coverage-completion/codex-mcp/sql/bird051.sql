SELECT ROUND(
    100.0 * COUNT(*) FILTER (
        WHERE c.uuid IN (
            SELECT f.uuid
            FROM main.foreign_data AS f
            WHERE f.language = 'Chinese Simplified'
        )
    ) / NULLIF(COUNT(*), 0),
    2
) AS percentage
FROM main.cards AS c
LIMIT 1
