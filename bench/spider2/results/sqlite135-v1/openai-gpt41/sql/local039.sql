WITH eligible_cities AS (
    SELECT city_id
    FROM city
    WHERE city LIKE 'A%' OR city LIKE '%-%'
),
rentals_with_duration AS (
    SELECT r.rental_id, r.inventory_id, r.rental_date, r.return_date, a.city_id
    FROM rental r
    JOIN customer c ON r.customer_id = c.customer_id
    JOIN address a ON c.address_id = a.address_id
    WHERE a.city_id IN (SELECT city_id FROM eligible_cities)
      AND r.return_date IS NOT NULL
),
film_categories_with_rental_hours AS (
    SELECT
        fc.category_id,
        julianday(rwd.return_date) - julianday(rwd.rental_date) AS rental_hours
    FROM rentals_with_duration rwd
    JOIN inventory i ON rwd.inventory_id = i.inventory_id
    JOIN film f ON i.film_id = f.film_id
    JOIN film_category fc ON f.film_id = fc.film_id
)
SELECT
    c.name AS category,
    SUM(fcw_rh.rental_hours * 24.0) AS total_rental_hours
FROM film_categories_with_rental_hours fcw_rh
JOIN category c ON fcw_rh.category_id = c.category_id
GROUP BY c.category_id, c.name
ORDER BY total_rental_hours DESC
LIMIT 1;
