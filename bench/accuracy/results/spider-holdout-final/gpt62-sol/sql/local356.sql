WITH excluded_laps AS MATERIALIZED (
    SELECT race_id, driver_id, lap FROM pit_stops
    UNION
    SELECT race_id, driver_id, lap + 1 FROM pit_stops
    UNION
    SELECT race_id, driver_id, lap FROM retirements
), relative_positions AS (
    SELECT curr.driver_id,
           ROW_NUMBER() OVER (
               PARTITION BY curr.race_id, curr.lap ORDER BY prev.position
           ) AS previous_rank,
           ROW_NUMBER() OVER (
               PARTITION BY curr.race_id, curr.lap ORDER BY curr.position
           ) AS current_rank
    FROM lap_times AS curr
    JOIN lap_times AS prev
      ON prev.race_id = curr.race_id
     AND prev.driver_id = curr.driver_id
     AND prev.lap = curr.lap - 1
    LEFT JOIN excluded_laps AS x
      ON x.race_id = curr.race_id
     AND x.driver_id = curr.driver_id
     AND x.lap = curr.lap
    WHERE curr.lap > 1
      AND x.race_id IS NULL
), net_losses AS (
    SELECT driver_id
    FROM relative_positions
    GROUP BY driver_id
    HAVING SUM(current_rank - previous_rank) > 0
)
SELECT d.full_name
FROM net_losses AS n
JOIN drivers AS d ON d.driver_id = n.driver_id
ORDER BY d.full_name;
