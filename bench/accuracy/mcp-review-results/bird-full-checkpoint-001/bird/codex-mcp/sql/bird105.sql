WITH per_set AS (
    SELECT s.id, COUNT(DISTINCT t.language) AS language_count
    FROM main.sets AS s
    LEFT JOIN main.set_translations AS t
        ON t.setCode = s.code
       AND TRIM(t.translation) <> ''
       AND TRIM(t.language) <> ''
    WHERE s.releaseDate >= '2016-01-01'
      AND s.releaseDate < '2017-01-01'
    GROUP BY s.id
)
SELECT ROUND(AVG(language_count), 2) AS average_languages
FROM per_set
LIMIT 1
