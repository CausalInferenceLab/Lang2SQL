WITH film_revenue AS (
    SELECT i.film_id, SUM(p.amount) AS total_revenue
    FROM payment AS p
    JOIN rental AS r ON r.rental_id = p.rental_id
    JOIN inventory AS i ON i.inventory_id = r.inventory_id
    GROUP BY i.film_id
),
cast_size AS (
    SELECT film_id, COUNT(*) AS actor_count
    FROM film_actor
    GROUP BY film_id
),
ranked_films AS (
    SELECT fa.actor_id,
           f.film_id,
           f.title,
           COALESCE(fr.total_revenue, 0) AS total_revenue,
           1.0 * COALESCE(fr.total_revenue, 0) / cs.actor_count AS average_revenue_per_actor,
           ROW_NUMBER() OVER (
               PARTITION BY fa.actor_id
               ORDER BY COALESCE(fr.total_revenue, 0) DESC, f.film_id
           ) AS revenue_rank
    FROM film_actor AS fa
    JOIN film AS f ON f.film_id = fa.film_id
    JOIN cast_size AS cs ON cs.film_id = fa.film_id
    LEFT JOIN film_revenue AS fr ON fr.film_id = fa.film_id
)
SELECT a.actor_id,
       a.first_name,
       a.last_name,
       rf.revenue_rank,
       rf.film_id,
       rf.title,
       rf.total_revenue,
       rf.average_revenue_per_actor
FROM ranked_films AS rf
JOIN actor AS a ON a.actor_id = rf.actor_id
WHERE rf.revenue_rank <= 3
ORDER BY a.actor_id, rf.revenue_rank;
