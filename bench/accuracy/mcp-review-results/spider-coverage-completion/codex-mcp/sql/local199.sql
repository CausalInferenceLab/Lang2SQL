WITH monthly_rentals AS (
    SELECT
        s.store_id,
        CAST(strftime('%Y', r.rental_date) AS INTEGER) AS year,
        CAST(strftime('%m', r.rental_date) AS INTEGER) AS month,
        COUNT(*) AS total_rentals
    FROM main.rental AS r
    JOIN main.staff AS s ON s.staff_id = r.staff_id
    GROUP BY s.store_id, year, month
),
ranked_months AS (
    SELECT
        monthly_rentals.*,
        RANK() OVER (
            PARTITION BY store_id
            ORDER BY total_rentals DESC
        ) AS rental_rank
    FROM monthly_rentals
)
SELECT store_id, year, month, total_rentals
FROM ranked_months
WHERE rental_rank = 1
ORDER BY store_id, year, month
LIMIT 1000
