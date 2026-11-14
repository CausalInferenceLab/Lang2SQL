WITH film_revenue AS (
    SELECT
        f.film_id,
        f.title,
        SUM(p.amount) AS total_revenue
    FROM film f
    JOIN inventory i ON f.film_id = i.film_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    JOIN payment p ON r.rental_id = p.rental_id
    GROUP BY f.film_id
),
film_actor_count AS (
    SELECT
        film_id,
        COUNT(actor_id) AS actor_count
    FROM film_actor
    GROUP BY film_id
),
film_revenue_per_actor AS (
    SELECT
        fr.film_id,
        fr.title,
        fr.total_revenue,
        fac.actor_count,
        (CASE WHEN fac.actor_count = 0 THEN 0 ELSE 1.0 * fr.total_revenue / fac.actor_count END) AS avg_revenue_per_actor
    FROM film_revenue fr
    JOIN film_actor_count fac ON fr.film_id = fac.film_id
),
actor_film_revenues AS (
    SELECT
        a.actor_id,
        a.first_name || ' ' || a.last_name AS actor_name,
        fra.film_id,
        fra.title,
        fra.total_revenue,
        fra.avg_revenue_per_actor,
        ROW_NUMBER() OVER (
            PARTITION BY a.actor_id
            ORDER BY fra.total_revenue DESC, fra.film_id
        ) AS rn
    FROM actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
    JOIN film_revenue_per_actor fra ON fa.film_id = fra.film_id
)
SELECT
    actor_id,
    actor_name,
    film_id,
    title AS film_title,
    total_revenue,
    avg_revenue_per_actor
FROM actor_film_revenues
WHERE rn <= 3
ORDER BY actor_id, total_revenue DESC, film_id;
