WITH excluded_laps AS (
    SELECT race_id, driver_id, lap FROM main.pit_stops
    UNION
    SELECT race_id, driver_id, lap + 1 FROM main.pit_stops
    UNION
    SELECT race_id, driver_id, lap FROM main.retirements
),
eligible_laps AS MATERIALIZED (
    SELECT cur.race_id, cur.lap, cur.driver_id,
           prev.position AS position_before,
           cur.position AS position_after
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
      AND r.is_pit_data_available = 1
      AND x.driver_id IS NULL
),
passes AS MATERIALIZED (
    SELECT a.driver_id AS passer, b.driver_id AS passed
    FROM eligible_laps AS a
    JOIN eligible_laps AS b
      ON b.race_id = a.race_id
     AND b.lap = a.lap
     AND a.position_before > b.position_before
     AND a.position_after < b.position_after
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
