WITH first_win AS (
    SELECT d.driverId,
           d.forename || ' ' || d.surname AS full_name,
           d.nationality,
           r.name AS first_win_name,
           r.date AS first_win_date
    FROM drivers AS d
    JOIN results AS res ON res.driverId = d.driverId
    JOIN races AS r ON r.raceId = res.raceId
    WHERE res.position = 1
      AND d.dob IS NOT NULL
      AND r.year BETWEEN 1950 AND 2017
    ORDER BY d.dob, full_name, d.driverId, r.date, r.name, r.raceId
    LIMIT 1
)
SELECT w.full_name,
       w.nationality,
       w.first_win_name,
       w.first_win_date,
       (
           SELECT COUNT(DISTINCT res.raceId)
           FROM results AS res
           JOIN races AS r ON r.raceId = res.raceId
           WHERE res.driverId = w.driverId
             AND r.year BETWEEN 1950 AND 2017
             AND NOT EXISTS (
                 SELECT 1
                 FROM status AS s
                 WHERE s.statusId = res.statusId
                   AND s.status IN ('Did not start', 'Withdrew', 'Did not qualify',
                                    'Did not prequalify', '107% Rule')
             )
       ) AS total_races_started
FROM first_win AS w;
