WITH first_payment_per_customer AS (
    SELECT
        p.customer_id,
        MIN(p.payment_date) AS first_payment_date
    FROM payment p
    GROUP BY p.customer_id
),
first_rental_per_customer AS (
    SELECT
        fp.customer_id,
        r.rental_id,
        r.inventory_id,
        r.rental_date
    FROM first_payment_per_customer fp
    JOIN rental r
        ON fp.customer_id = r.customer_id
    WHERE r.rental_date = (
        SELECT MIN(r2.rental_date)
        FROM rental r2
        WHERE r2.customer_id = r.customer_id
    )
    -- In case multiple rentals have the same earliest date, limit to the first one only
    LIMIT 1
),
first_film_per_customer AS (
    SELECT
        frc.customer_id,
        i.film_id
    FROM first_rental_per_customer frc
    JOIN inventory i ON frc.inventory_id = i.inventory_id
),
first_film_rating_per_customer AS (
    SELECT
        ffc.customer_id,
        f.rating
    FROM first_film_per_customer ffc
    JOIN film f ON ffc.film_id = f.film_id
),
customer_total_payments AS (
    SELECT
        customer_id,
        SUM(amount) AS total_amount
    FROM payment
    GROUP BY customer_id
),
customer_total_rentals AS (
    SELECT
        customer_id,
        COUNT(*) AS rental_count
    FROM rental
    GROUP BY customer_id
),
per_customer_stats AS (
    SELECT
        ffpc.rating,
        ffpc.customer_id,
        ctp.total_amount,
        ctr.rental_count
    FROM first_film_rating_per_customer ffpc
    JOIN customer_total_payments ctp ON ffpc.customer_id = ctp.customer_id
    JOIN customer_total_rentals ctr ON ffpc.customer_id = ctr.customer_id
)
SELECT
    rating,
    AVG(total_amount) AS avg_total_amount_spent_per_customer,
    AVG(rental_count - 1) AS avg_subsequent_rentals_per_customer
FROM per_customer_stats
GROUP BY rating
ORDER BY rating;
