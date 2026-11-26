WITH printing_counts AS (
    SELECT c.id, COUNT(r.id) AS ruling_count
    FROM cards AS c
    LEFT JOIN rulings AS r ON r.uuid = c.uuid
    GROUP BY c.id
)
SELECT DISTINCT c.name, c.artist, c.isPromo
FROM cards AS c
JOIN printing_counts AS p ON p.id = c.id
WHERE p.ruling_count = (SELECT MAX(ruling_count) FROM printing_counts)
LIMIT 1000
