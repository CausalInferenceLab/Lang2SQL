WITH excluded_laps AS (
    SELECT race_id, driver_id, lap FROM main.pit_stops
    UNION
    SELECT race_id, driver_id, lap + 1 FROM main.pit_stops
    UNION
    SELECT race_id, driver_id, lap FROM main.retirements
),
race_laps AS MATERIALIZED (
    SELECT cur.race_id, cur.lap, cur.driver_id,
           prev.position AS previous_position, cur.position
    FROM main.lap_times AS cur
    JOIN main.lap_times AS prev
      ON prev.race_id = cur.race_id
     AND prev.driver_id = cur.driver_id
     AND prev.lap = cur.lap - 1
    JOIN main.races_ext AS r ON r.race_id = cur.race_id
    LEFT JOIN excluded_laps AS x
      ON x.race_id = cur.race_id
     AND x.driver_id = cur.driver_id
     AND x.lap = cur.lap
    WHERE cur.lap > 1
      
      AND x.driver_id IS NULL
),
passes AS MATERIALIZED (
    SELECT a.driver_id AS passer, b.driver_id AS passed
    FROM race_laps AS a
    JOIN race_laps AS b
      ON b.race_id = a.race_id
     AND b.lap = a.lap
     AND a.previous_position > b.previous_position
     AND a.position < b.position
),
driver_events AS (
    SELECT passed AS driver_id, 1 AS net_times_passed FROM passes
    UNION ALL
    SELECT passer AS driver_id, -1 AS net_times_passed FROM passes
)
SELECT d.full_name
FROM main.drivers_ext AS d
JOIN driver_events AS e ON e.driver_id = d.driver_id
GROUP BY d.driver_id, d.full_name
HAVING SUM(e.net_times_passed) > 0
ORDER BY d.full_name
LIMIT -1
