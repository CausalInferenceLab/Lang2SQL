WITH filtered_films AS (
  SELECT f.film_id
  FROM film f
  JOIN language l ON f.language_id = l.language_id
  JOIN film_category fc ON f.film_id = fc.film_id
  JOIN category c ON fc.category_id = c.category_id
  WHERE l.name = 'English'
    AND c.name = 'Children'
    AND f.rating IN ('G', 'PG')
    AND f.length <= 120
    AND f.release_year BETWEEN 2000 AND 2010
)
SELECT a.first_name || ' ' || a.last_name AS actor_full_name,
       COUNT(*) AS film_count
FROM actor a
JOIN film_actor fa ON a.actor_id = fa.actor_id
JOIN filtered_films ff ON fa.film_id = ff.film_id
GROUP BY a.actor_id
ORDER BY film_count DESC
LIMIT 1;
