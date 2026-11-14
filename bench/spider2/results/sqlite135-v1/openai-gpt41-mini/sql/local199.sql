WITH store_rental_counts AS (
    SELECT 
        s.store_id,
        STRFTIME('%Y', r.rental_date) AS year,
        STRFTIME('%m', r.rental_date) AS month,
        COUNT(*) AS total_rentals
    FROM rental r
    JOIN staff s ON r.staff_id = s.staff_id
    GROUP BY s.store_id, year, month
),
max_rentals AS (
    SELECT 
        store_id,
        MAX(total_rentals) AS max_total_rentals
    FROM store_rental_counts
    GROUP BY store_id
)
SELECT 
    src.store_id,
    src.year,
    src.month,
    src.total_rentals
FROM store_rental_counts src
JOIN max_rentals mr ON src.store_id = mr.store_id AND src.total_rentals = mr.max_total_rentals
ORDER BY src.store_id;
