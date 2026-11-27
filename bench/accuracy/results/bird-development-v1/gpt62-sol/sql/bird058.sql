WITH french_printings AS (
    SELECT DISTINCT uuid
    FROM foreign_data
    WHERE language = 'French'
)
SELECT ROUND(100.0 * COUNT(f.uuid) / NULLIF(COUNT(*), 0), 2) AS percentage_with_french_translation
FROM cards AS c
LEFT JOIN french_printings AS f ON f.uuid = c.uuid
WHERE c.power IS NULL OR TRIM(c.power) = '' OR c.power LIKE '%*%';
