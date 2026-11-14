WITH races_50s AS (
    SELECT raceId, year
    FROM races
    WHERE year BETWEEN 1950 AND 1959
),
driver_seasons AS (
    SELECT
        d.driverId,
        r.year,
        MIN(r.raceId) AS first_race_id,
        MAX(r.raceId) AS last_race_id
    FROM results res
    INNER JOIN races_50s r ON res.raceId = r.raceId
    INNER JOIN drivers d ON res.driverId = d.driverId
    GROUP BY d.driverId, r.year
    HAVING COUNT(DISTINCT res.raceId) >= 2
),
same_constructor AS (
    SELECT
        ds.driverId,
        ds.year
    FROM driver_seasons ds
    INNER JOIN results res_first
        ON res_first.driverId = ds.driverId
       AND res_first.raceId = ds.first_race_id
    INNER JOIN results res_last
        ON res_last.driverId = ds.driverId
       AND res_last.raceId = ds.last_race_id
    WHERE res_first.constructorId = res_last.constructorId
)
SELECT DISTINCT d.driverId, d.forename, d.surname, sc.year
FROM same_constructor sc
JOIN drivers d ON d.driverId = sc.driverId
ORDER BY d.surname, d.forename, sc.year;
