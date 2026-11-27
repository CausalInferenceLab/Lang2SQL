WITH race_wins AS (
    SELECT driverId, COUNT(DISTINCT raceId) AS win_count
    FROM results
    WHERE position = 1
    GROUP BY driverId
)
SELECT d.forename AS first_name,
       d.surname AS last_name,
       d.nationality,
       (SELECT MAX(ds.points)
        FROM driverStandings AS ds
        WHERE ds.driverId = d.driverId) AS highest_standings_points
FROM race_wins AS w
JOIN drivers AS d ON d.driverId = w.driverId
WHERE w.win_count = (SELECT MAX(win_count) FROM race_wins);
