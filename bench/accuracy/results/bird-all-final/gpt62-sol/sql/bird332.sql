WITH eye_colour_counts AS (
    SELECT c.colour AS eye_colour, COUNT(*) AS hero_count
    FROM superhero AS s
    JOIN publisher AS p ON p.id = s.publisher_id
    JOIN colour AS c ON c.id = s.eye_colour_id
    WHERE p.publisher_name = 'Marvel Comics'
      AND c.colour <> 'No Colour'
    GROUP BY c.colour
)
SELECT eye_colour, hero_count,
       RANK() OVER (ORDER BY hero_count DESC) AS rank
FROM eye_colour_counts
ORDER BY hero_count DESC, eye_colour ASC;
