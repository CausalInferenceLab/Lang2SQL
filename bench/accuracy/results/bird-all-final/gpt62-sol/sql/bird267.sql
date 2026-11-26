WITH french_editions AS (
    SELECT raceId, date
    FROM races
    WHERE name = 'French Grand Prix'
      AND year >= 1990
),
qualifying_laps AS (
    SELECT lt.raceId, lt.driverId, MIN(lt.milliseconds) AS fastest_ms
    FROM lapTimes AS lt
    JOIN french_editions AS e ON e.raceId = lt.raceId
    GROUP BY lt.raceId, lt.driverId
    HAVING MIN(lt.milliseconds) > 90000
)
SELECT e.date,
       d.forename,
       d.surname,
       CASE WHEN q.fastest_ms IS NOT NULL THEN
           printf('%d:%02d.%03d',
                  q.fastest_ms / 60000,
                  (q.fastest_ms / 1000) % 60,
                  q.fastest_ms % 1000)
       END AS fastest_lap_time
FROM french_editions AS e
LEFT JOIN qualifying_laps AS q ON q.raceId = e.raceId
LEFT JOIN drivers AS d ON d.driverId = q.driverId
ORDER BY e.date, d.surname, d.forename;
