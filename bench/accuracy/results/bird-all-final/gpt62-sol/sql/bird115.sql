WITH eligible_sets AS (
    SELECT id, code, name, totalSetSize, isForeignOnly
    FROM sets
    WHERE type = 'commander'
      AND releaseDate >= '2020-01-01'
      AND releaseDate < '2021-01-01'
), top_sets AS (
    SELECT id, code, name, isForeignOnly
    FROM eligible_sets
    WHERE totalSetSize = (SELECT MAX(totalSetSize) FROM eligible_sets)
), release_languages AS (
    SELECT code, 'English' AS language
    FROM top_sets
    WHERE isForeignOnly = 0
    UNION
    SELECT c.setCode, fd.language
    FROM top_sets AS s
    JOIN cards AS c ON c.setCode = s.code
    JOIN foreign_data AS fd ON fd.uuid = c.uuid
)
SELECT s.id, s.name, COUNT(l.language) AS language_count
FROM top_sets AS s
LEFT JOIN release_languages AS l ON l.code = s.code
GROUP BY s.id, s.name
ORDER BY s.id ASC;
