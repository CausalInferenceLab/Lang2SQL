WITH actor_counts AS (
    SELECT a.actor_id,
           a.first_name || ' ' || a.last_name AS full_name,
           COUNT(DISTINCT f.film_id) AS film_count
    FROM actor AS a
    JOIN film_actor AS fa ON fa.actor_id = a.actor_id
    JOIN film AS f ON f.film_id = fa.film_id
    JOIN language AS l ON l.language_id = f.language_id
    JOIN film_category AS fc ON fc.film_id = f.film_id
    JOIN category AS c ON c.category_id = fc.category_id
    WHERE TRIM(l.name) = 'English'
      AND c.name = 'Children'
      AND f.rating IN ('G', 'PG')
      AND f.length <= 120
      AND CAST(f.release_year AS INTEGER) BETWEEN 2000 AND 2010
    GROUP BY a.actor_id, a.first_name, a.last_name
)
SELECT full_name
FROM actor_counts
WHERE film_count = (SELECT MAX(film_count) FROM actor_counts)
ORDER BY full_name;
