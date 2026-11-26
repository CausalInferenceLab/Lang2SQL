SELECT COUNT(*) AS hero_count
FROM main.superhero AS hero
WHERE hero.publisher_id = (
    SELECT blackwulf.publisher_id
    FROM main.superhero AS blackwulf
    WHERE blackwulf.superhero_name = 'Blackwulf'
)
AND EXISTS (
    SELECT 1
    FROM main.superhero AS other
    JOIN main.publisher AS p ON p.id = other.publisher_id
    WHERE other.eye_colour_id = hero.eye_colour_id
      AND other.publisher_id <> hero.publisher_id
      AND p.publisher_name <> ''
)
LIMIT 1
