SELECT ROUND(
    100.0 * COUNT(CASE WHEN c.uuid IN (
        SELECT fd.uuid
        FROM foreign_data AS fd
        WHERE fd.language = 'Chinese Simplified'
    ) THEN 1 END) / COUNT(*),
    2
) AS percentage
FROM cards AS c;
