WITH filtered_cities AS (
    SELECT city_id 
    FROM city 
    WHERE city LIKE 'A%' OR city LIKE '%-%'
),
rentals_in_cities AS (
    SELECT r.rental_id, f.film_id, f.film_category_id, r.return_date, r.rental_date
    FROM rental r
    JOIN inventory i ON r.inventory_id = i.inventory_id
    JOIN film f ON i.film_id = f.film_id
    JOIN store s ON i.store_id = s.store_id
    JOIN address a ON s.address_id = a.address_id
    WHERE a.city_id IN (SELECT city_id FROM filtered_cities)
),
rental_hours_by_category AS (
    SELECT fc.category_id, SUM(
        (julianday(r.return_date) - julianday(r.rental_date)) * 24
    ) AS total_rental_hours
    FROM rentals_in_cities r
    JOIN film_category fc ON r.film_id = fc.film_id
    GROUP BY fc.category_id
)
SELECT c.name AS film_category, rh.total_rental_hours
FROM rental_hours_by_category rh
JOIN category c ON rh.category_id = c.category_id
ORDER BY rh.total_rental_hours DESC
LIMIT 1;
