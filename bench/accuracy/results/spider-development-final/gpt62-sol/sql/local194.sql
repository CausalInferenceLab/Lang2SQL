WITH cast_members AS (
    SELECT DISTINCT film_id, actor_id
    FROM film_actor
), cast_sizes AS (
    SELECT film_id, COUNT(*) AS actor_count
    FROM cast_members
    GROUP BY film_id
), film_revenues AS (
    SELECT f.film_id, f.title,
           ROUND(TOTAL(p.amount), 2) AS total_revenue
    FROM film AS f
    LEFT JOIN inventory AS i ON i.film_id = f.film_id
    LEFT JOIN rental AS r ON r.inventory_id = i.inventory_id
    LEFT JOIN payment AS p ON p.rental_id = r.rental_id
    GROUP BY f.film_id, f.title
), ranked_films AS (
    SELECT cm.actor_id, fr.title,
           fr.total_revenue / cs.actor_count AS average_revenue_per_actor,
           ROW_NUMBER() OVER (
               PARTITION BY cm.actor_id
               ORDER BY fr.total_revenue DESC, fr.film_id
           ) AS revenue_rank
    FROM cast_members AS cm
    JOIN cast_sizes AS cs ON cs.film_id = cm.film_id
    JOIN film_revenues AS fr ON fr.film_id = cm.film_id
)
SELECT a.first_name || ' ' || a.last_name AS actor_name,
       rf.title AS film_title,
       rf.average_revenue_per_actor
FROM ranked_films AS rf
JOIN actor AS a ON a.actor_id = rf.actor_id
WHERE rf.revenue_rank <= 3
ORDER BY a.last_name, a.first_name, a.actor_id, rf.revenue_rank;
