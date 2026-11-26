WITH driver_q3_counts AS (
    SELECT d.driverId, COUNT(NULLIF(TRIM(q.q3), '')) AS q3_count
    FROM drivers AS d
    LEFT JOIN qualifying AS q ON q.driverId = d.driverId
    WHERE d.dob >= '1981-01-01' AND d.dob < '1992-01-01'
    GROUP BY d.driverId
)
SELECT ROUND(AVG(q3_count), 2) AS average_q3_lap_times
FROM driver_q3_counts;
