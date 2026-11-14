WITH TopActors AS (
    SELECT actor_id
    FROM film_actor
    GROUP BY actor_id
    ORDER BY COUNT(film_id) DESC
    LIMIT 5
),
CustomerWithTopActorRentals AS (
    SELECT DISTINCT c.customer_id
    FROM customer c
    JOIN rental r ON c.customer_id = r.customer_id
    JOIN inventory i ON r.inventory_id = i.inventory_id
    JOIN film_actor fa ON i.film_id = fa.film_id
    WHERE fa.actor_id IN (SELECT actor_id FROM TopActors)
),
TotalCustomers AS (
    SELECT COUNT(*) AS total_customers FROM customer
),
TotalWithTopActors AS (
    SELECT COUNT(*) AS customers_with_top_actors FROM CustomerWithTopActorRentals
)
SELECT
    CAST(customers_with_top_actors AS FLOAT) / total_customers * 100 AS percentage_of_customers_renting_top_actor_films
FROM TotalWithTopActors, TotalCustomers
;
