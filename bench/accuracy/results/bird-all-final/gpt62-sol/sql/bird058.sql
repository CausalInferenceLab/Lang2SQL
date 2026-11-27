WITH french_printings AS (
    SELECT DISTINCT uuid
    FROM foreign_data
    WHERE language = 'French'
)
SELECT ROUND(100.0 * COUNT(f.uuid) / NULLIF(COUNT(*), 0), 2) AS french_translation_percentage
FROM cards AS c
LEFT JOIN french_printings AS f ON f.uuid = c.uuid
WHERE c.power IS NULL OR INSTR(c.power, '*') > 0;
