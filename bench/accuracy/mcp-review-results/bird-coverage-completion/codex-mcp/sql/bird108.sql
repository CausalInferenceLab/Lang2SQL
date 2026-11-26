WITH set_languages AS (
    SELECT DISTINCT language
    FROM main.set_translations
    WHERE TRIM(translation) <> ''
),
language_counts AS (
    SELECT s.language, COUNT(DISTINCT f.uuid) AS printing_count
    FROM set_languages AS s
    LEFT JOIN main.foreign_data AS f
        ON f.language = s.language
       AND TRIM(f.flavorText) <> ''
    GROUP BY s.language
)
SELECT language, printing_count
FROM language_counts
WHERE printing_count = (SELECT MIN(printing_count) FROM language_counts)
ORDER BY language
LIMIT -1
