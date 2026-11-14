WITH film_revenue AS (
  SELECT f.film_id, f.title, SUM(p.amount) AS total_revenue
  FROM film f
  JOIN inventory i ON f.film_id = i.film_id
  JOIN rental r ON i.inventory_id = r.inventory_id
  JOIN payment p ON r.rental_id = p.rental_id
  GROUP BY f.film_id
), actors_per_film AS (
  SELECT fa.film_id, fa.actor_id, a.first_name || ' ' || a.last_name AS actor_name
  FROM film_actor fa
  JOIN actor a ON fa.actor_id = a.actor_id
), actor_count_per_film AS (
  SELECT film_id, COUNT(actor_id) AS actor_count
  FROM film_actor
  GROUP BY film_id
), film_actor_revenue AS (
  SELECT a.film_id, a.actor_id, a.actor_name, fr.title,
         fr.total_revenue, ac.actor_count,
         fr.total_revenue * 1.0 / ac.actor_count AS revenue_per_actor
  FROM actors_per_film a
  JOIN film_revenue fr ON a.film_id = fr.film_id
  JOIN actor_count_per_film ac ON a.film_id = ac.film_id
), ranked_films AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY actor_id ORDER BY revenue_per_actor DESC) AS rn
  FROM film_actor_revenue
), top3_films_per_actor AS (
  SELECT film_id, actor_id, actor_name, title, revenue_per_actor
  FROM ranked_films
  WHERE rn <= 3
), avg_revenue_per_actor AS (
  SELECT actor_id, AVG(revenue_per_actor) AS avg_revenue
  FROM top3_films_per_actor
  GROUP BY actor_id
)
SELECT t.actor_id, t.actor_name, t.film_id, t.title, t.revenue_per_actor, a.avg_revenue
FROM top3_films_per_actor t
JOIN avg_revenue_per_actor a ON t.actor_id = a.actor_id
ORDER BY t.actor_id, t.revenue_per_actor DESC;
