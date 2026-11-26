SELECT COUNT(*) AS hero_count
FROM superhero AS h
WHERE h.publisher_id IN (
    SELECT b.publisher_id
    FROM superhero AS b
    WHERE b.superhero_name = 'Blackwulf'
)
AND EXISTS (
    SELECT 1
    FROM superhero AS other
    JOIN publisher AS p ON p.id = other.publisher_id
    WHERE other.publisher_id <> h.publisher_id
      AND NULLIF(TRIM(p.publisher_name), '') IS NOT NULL
      AND other.eye_colour_id = h.eye_colour_id
);
