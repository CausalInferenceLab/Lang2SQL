WITH rental_counts AS (
  SELECT
    store.store_id,
    strftime('%Y', rental.rental_date) AS year,
    strftime('%m', rental.rental_date) AS month,
    COUNT(*) AS total_rentals
  FROM rental
  JOIN staff ON rental.staff_id = staff.staff_id
  JOIN store ON staff.store_id = store.store_id
  GROUP BY store.store_id, year, month
),
max_rentals AS (
  SELECT
    store_id,
    MAX(total_rentals) AS max_total_rentals
  FROM rental_counts
  GROUP BY store_id
)
SELECT
  rc.store_id,
  rc.year,
  rc.month,
  rc.total_rentals
FROM rental_counts rc
JOIN max_rentals mr
  ON rc.store_id = mr.store_id
 AND rc.total_rentals = mr.max_total_rentals
ORDER BY rc.store_id, rc.year, rc.month;
