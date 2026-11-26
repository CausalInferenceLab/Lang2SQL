WITH qualifying_drivers AS (
    SELECT DISTINCT first_drive.driver_id
    FROM main.drives AS first_drive
    JOIN main.drives AS last_drive
      ON last_drive.driver_id = first_drive.driver_id
     AND last_drive.year = first_drive.year
     AND last_drive.constructor_id = first_drive.constructor_id
    WHERE first_drive.year BETWEEN 1950 AND 1959
      AND first_drive.is_first_drive_of_season = 1
      AND last_drive.is_final_drive_of_season = 1
      AND first_drive.first_round < last_drive.last_round
)
SELECT d.full_name
FROM qualifying_drivers AS q
JOIN main.drivers_ext AS d ON d.driver_id = q.driver_id
ORDER BY d.full_name
LIMIT 1000
