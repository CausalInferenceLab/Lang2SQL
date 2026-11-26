WITH actor_counts AS (
    SELECT fa.actor_id, COUNT(DISTINCT f.film_id) AS film_count
    FROM main.film_actor AS fa
    JOIN main.film AS f ON f.film_id = fa.film_id
    JOIN main.language AS l ON l.language_id = f.language_id
    JOIN main.film_category AS fc ON fc.film_id = f.film_id
    JOIN main.category AS c ON c.category_id = fc.category_id
    WHERE l.name = 'English'
      AND c.name = 'Children'
      AND f.rating IN ('G', 'PG')
      AND f.length <= 120
      AND f.release_year BETWEEN '2000' AND '2010'
    GROUP BY fa.actor_id
)
SELECT a.first_name || ' ' || a.last_name AS full_name
FROM actor_counts AS ac
JOIN main.actor AS a ON a.actor_id = ac.actor_id
WHERE ac.film_count = (SELECT MAX(film_count) FROM actor_counts)
ORDER BY full_name
LIMIT 1000
