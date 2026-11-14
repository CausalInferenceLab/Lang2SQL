WITH FirstRental AS (
    SELECT r.customer_id, r.inventory_id, MIN(p.payment_date) AS first_payment_date
    FROM rental r
    JOIN payment p ON r.rental_id = p.rental_id
    GROUP BY r.customer_id
),
FirstRentalDetails AS (
    SELECT fr.customer_id, i.film_id
    FROM FirstRental fr
    JOIN inventory i ON fr.inventory_id = i.inventory_id
),
FirstRentalWithRating AS (
    SELECT frd.customer_id, f.rating
    FROM FirstRentalDetails frd
    JOIN film f ON frd.film_id = f.film_id
),
CustomerStats AS (
    SELECT r.customer_id,
           COUNT(*) AS total_rentals,
           SUM(p.amount) AS total_amount_spent
    FROM rental r
    JOIN payment p ON r.rental_id = p.rental_id
    GROUP BY r.customer_id
)
SELECT f.rating,
       AVG(cs.total_amount_spent) AS avg_total_amount_spent_per_customer,
       AVG(cs.total_rentals - 1) AS avg_number_of_subsequent_rentals
FROM FirstRentalWithRating f
JOIN CustomerStats cs ON f.customer_id = cs.customer_id
GROUP BY f.rating
ORDER BY f.rating;
