WITH race_stop_counts AS (
    SELECT r.raceId, COUNT(p.raceId) AS pit_stops
    FROM main.races AS r
    LEFT JOIN main.pitStops AS p ON p.raceId = r.raceId
    WHERE r.year = 2017
      AND r.name NOT IN (
          SELECT prior.name
          FROM main.races AS prior
          WHERE prior.year = 2000
      )
    GROUP BY r.raceId
)
SELECT ROUND(AVG(pit_stops), 2) AS average_pit_stops_per_race
FROM race_stop_counts
LIMIT 1000
