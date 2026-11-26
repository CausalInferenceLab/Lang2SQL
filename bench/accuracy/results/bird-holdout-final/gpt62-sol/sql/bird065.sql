WITH translation_counts AS (
    SELECT setCode, COUNT(DISTINCT translation COLLATE BINARY) AS translation_count
    FROM set_translations
    GROUP BY setCode
)
SELECT SUM(s.totalSetSize) AS total_cards
FROM sets AS s
LEFT JOIN translation_counts AS t ON t.setCode = s.code
WHERE COALESCE(t.translation_count, 0) < 5;
