WITH ruling_counts AS (
    SELECT c.uuid, COUNT(r.uuid) AS ruling_count
    FROM cards AS c
    LEFT JOIN rulings AS r ON r.uuid = c.uuid
    GROUP BY c.uuid
)
SELECT DISTINCT c.name, c.artist, c.isPromo
FROM cards AS c
JOIN ruling_counts AS rc ON rc.uuid = c.uuid
WHERE rc.ruling_count = (SELECT MAX(ruling_count) FROM ruling_counts)
ORDER BY c.name, c.artist, c.isPromo;
