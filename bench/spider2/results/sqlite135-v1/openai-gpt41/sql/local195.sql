WITH top_actors AS (
  SELECT fa.actor_id, COUNT(*) AS film_count
  FROM film_actor fa
  GROUP BY fa.actor_id
  ORDER BY film_count DESC
  LIMIT 5
),
actor_films AS (
  SELECT fa.actor_id, fa.film_id
  FROM film_actor fa
  JOIN top_actors ta ON fa.actor_id = ta.actor_id
),
actor_rentals AS (
  SELECT DISTINCT r.customer_id
  FROM rental r
  JOIN inventory i ON r.inventory_id = i.inventory_id
  JOIN actor_films af ON i.film_id = af.film_id
),
total_customers AS (
  SELECT COUNT(DISTINCT customer_id) AS total_cnt FROM customer
)
SELECT 
  ROUND(CAST(COUNT(DISTINCT ar.customer_id) AS FLOAT) / tc.total_cnt * 100, 2) AS pct_customers_rented_top5_actor_films
FROM actor_rentals ar
CROSS JOIN total_customers tc
