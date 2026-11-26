WITH cast_members AS (
    SELECT DISTINCT film_id, actor_id
    FROM film_actor
),
cast_sizes AS (
    SELECT film_id, COUNT(*) AS actor_count
    FROM cast_members
    GROUP BY film_id
),
film_revenues AS (
    SELECT i.film_id, ROUND(SUM(p.amount), 2) AS total_revenue
    FROM payment AS p
    JOIN rental AS r ON r.rental_id = p.rental_id
    JOIN inventory AS i ON i.inventory_id = r.inventory_id
    GROUP BY i.film_id
),
ranked_films AS (
    SELECT cm.actor_id,
           cm.film_id,
           COALESCE(fr.total_revenue, 0.0) / cs.actor_count AS average_revenue_per_actor,
           ROW_NUMBER() OVER (
               PARTITION BY cm.actor_id
               ORDER BY COALESCE(fr.total_revenue, 0.0) DESC, cm.film_id
           ) AS film_rank
    FROM cast_members AS cm
    JOIN cast_sizes AS cs ON cs.film_id = cm.film_id
    LEFT JOIN film_revenues AS fr ON fr.film_id = cm.film_id
)
SELECT a.actor_id,
       a.first_name || ' ' || a.last_name AS actor_name,
       f.title AS film_title,
       rf.average_revenue_per_actor
FROM ranked_films AS rf
JOIN actor AS a ON a.actor_id = rf.actor_id
JOIN film AS f ON f.film_id = rf.film_id
WHERE rf.film_rank <= 3
ORDER BY a.actor_id, rf.film_rank;
