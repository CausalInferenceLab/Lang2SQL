WITH publisher_counts AS (
    SELECT p.publisher_name, COUNT(s.id) AS superhero_count
    FROM publisher AS p
    LEFT JOIN superhero AS s ON s.publisher_id = p.id
    WHERE p.publisher_name IN ('Marvel Comics', 'DC Comics')
    GROUP BY p.publisher_name
), comparison AS (
    SELECT publisher_name, superhero_count,
           MAX(superhero_count) OVER () AS highest_count,
           MIN(superhero_count) OVER () AS lowest_count
    FROM publisher_counts
)
SELECT publisher_name, highest_count - lowest_count AS difference
FROM comparison
WHERE superhero_count = highest_count;
