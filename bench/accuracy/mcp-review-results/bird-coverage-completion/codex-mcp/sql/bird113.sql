WITH language_counts AS (
    SELECT language, COUNT(*) AS printing_count
    FROM main.foreign_data
    WHERE language IN (SELECT language FROM main.set_translations)
    GROUP BY language
)
SELECT language, printing_count
FROM language_counts
WHERE printing_count = (SELECT MIN(printing_count) FROM language_counts)
ORDER BY language
LIMIT 1000
