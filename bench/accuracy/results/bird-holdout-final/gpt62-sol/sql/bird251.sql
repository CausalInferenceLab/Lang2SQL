WITH pit_stops_per_race AS (
    SELECT r.raceId, COUNT(p.raceId) AS pit_stop_count
    FROM races AS r
    LEFT JOIN pitStops AS p ON p.raceId = r.raceId
    WHERE r.year = 2017
      AND NOT EXISTS (
          SELECT 1
          FROM races AS r2000
          WHERE r2000.year = 2000
            AND r2000.name = r.name
      )
    GROUP BY r.raceId
)
SELECT ROUND(AVG(pit_stop_count), 2) AS average_pit_stops_per_race
FROM pit_stops_per_race;
