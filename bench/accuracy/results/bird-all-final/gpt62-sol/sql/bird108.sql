WITH set_languages AS (
    SELECT DISTINCT language
    FROM set_translations
    WHERE language IS NOT NULL
      AND TRIM(translation) <> ''
), printing_counts AS (
    SELECT sl.language, COUNT(DISTINCT fd.uuid) AS printing_count
    FROM set_languages AS sl
    LEFT JOIN foreign_data AS fd
      ON fd.language = sl.language
     AND TRIM(fd.flavorText) <> ''
    GROUP BY sl.language
)
SELECT language, printing_count
FROM printing_counts
WHERE printing_count = (SELECT MIN(printing_count) FROM printing_counts)
ORDER BY language;
