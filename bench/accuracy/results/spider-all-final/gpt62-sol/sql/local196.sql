WITH ranked_rental_payments AS (
    SELECT
        p.customer_id,
        r.inventory_id,
        ROW_NUMBER() OVER (
            PARTITION BY p.customer_id
            ORDER BY p.payment_date, p.payment_id
        ) AS payment_rank
    FROM payment AS p
    JOIN rental AS r
        ON r.rental_id = p.rental_id
       AND r.customer_id = p.customer_id
), customer_spending AS (
    SELECT customer_id, SUM(amount) AS total_amount_spent
    FROM payment
    GROUP BY customer_id
), customer_rentals AS (
    SELECT customer_id, COUNT(*) - 1 AS subsequent_rentals
    FROM rental
    GROUP BY customer_id
)
SELECT
    f.rating,
    AVG(s.total_amount_spent) AS avg_total_amount_spent,
    AVG(cr.subsequent_rentals) AS avg_subsequent_rentals
FROM ranked_rental_payments AS rp
JOIN inventory AS i ON i.inventory_id = rp.inventory_id
JOIN film AS f ON f.film_id = i.film_id
JOIN customer_spending AS s ON s.customer_id = rp.customer_id
JOIN customer_rentals AS cr ON cr.customer_id = rp.customer_id
WHERE rp.payment_rank = 1
GROUP BY f.rating
ORDER BY f.rating;
