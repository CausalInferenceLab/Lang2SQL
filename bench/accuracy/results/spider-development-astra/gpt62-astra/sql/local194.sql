WITH film_revenue AS (
    SELECT f.film_id, f.title, ROUND(TOTAL(p.amount), 2) AS total_revenue
    FROM film AS f
    LEFT JOIN inventory AS i ON i.film_id = f.film_id
    LEFT JOIN rental AS r ON r.inventory_id = i.inventory_id
    LEFT JOIN payment AS p ON p.rental_id = r.rental_id
    GROUP BY f.film_id, f.title
),
credits AS (
    SELECT DISTINCT actor_id, film_id
    FROM film_actor
),
ranked AS (
    SELECT c.actor_id, fr.title,
           fr.total_revenue / COUNT(*) OVER (PARTITION BY c.film_id) AS average_revenue_per_actor,
           ROW_NUMBER() OVER (
               PARTITION BY c.actor_id
               ORDER BY fr.total_revenue DESC, c.film_id
           ) AS film_rank
    FROM credits AS c
    JOIN film_revenue AS fr ON fr.film_id = c.film_id
)
SELECT a.first_name || ' ' || a.last_name AS actor_name,
       r.title, r.average_revenue_per_actor
FROM ranked AS r
JOIN actor AS a ON a.actor_id = r.actor_id
WHERE r.film_rank <= 3
ORDER BY a.last_name, a.first_name, a.actor_id, r.film_rank;
