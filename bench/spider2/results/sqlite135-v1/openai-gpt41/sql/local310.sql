WITH driver_points_by_year AS (
    SELECT year, driverId, SUM(points) AS total_driver_points
    FROM results
    GROUP BY year, driverId
),
max_driver_points_per_year AS (
    SELECT year, MAX(total_driver_points) AS max_driver_points
    FROM driver_points_by_year
    GROUP BY year
),
constructor_points_by_year AS (
    SELECT year, constructorId, SUM(points) AS total_constructor_points
    FROM results
    GROUP BY year, constructorId
),
max_constructor_points_per_year AS (
    SELECT year, MAX(total_constructor_points) AS max_constructor_points
    FROM constructor_points_by_year
    GROUP BY year
)
SELECT mdy.year,
       (mdy.max_driver_points + mcy.max_constructor_points) AS combined_total
FROM max_driver_points_per_year mdy
JOIN max_constructor_points_per_year mcy ON mdy.year = mcy.year
ORDER BY combined_total ASC, mdy.year ASC
LIMIT 3;
