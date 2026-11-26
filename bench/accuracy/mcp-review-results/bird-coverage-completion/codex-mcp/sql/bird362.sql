WITH race_counts AS (
    SELECT r.race, COUNT(*) AS superhero_count
    FROM main.superhero AS s
    JOIN main.race AS r ON r.id = s.race_id
    WHERE s.publisher_id IN (
        SELECT publisher_id
        FROM main.superhero
        WHERE full_name = 'Charles Chandler'
    )
      AND r.race NOT IN ('', '-')
    GROUP BY r.race
)
SELECT race, superhero_count
FROM race_counts
WHERE superhero_count = (SELECT MAX(superhero_count) FROM race_counts)
ORDER BY race
LIMIT 1000
