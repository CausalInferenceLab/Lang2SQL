WITH translated_sets AS (
    SELECT DISTINCT setCode
    FROM main.set_translations
    WHERE TRIM(translation) <> ''
)
SELECT
    s.type AS set_type,
    COUNT(t.setCode) AS translated_sets,
    SUM(CASE WHEN t.setCode IS NOT NULL THEN s.totalSetSize ELSE 0 END) AS total_cards
FROM main.sets AS s
LEFT JOIN translated_sets AS t ON t.setCode = s.code
GROUP BY s.type
ORDER BY s.type ASC
LIMIT 1000
