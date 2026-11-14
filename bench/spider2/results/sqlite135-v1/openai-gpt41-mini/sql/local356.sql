WITH position_changes AS (
  SELECT
    race_id,
    driver_id,
    lap,
    position AS current_position,
    LAG(position) OVER (PARTITION BY race_id, driver_id ORDER BY lap) AS previous_position
  FROM lap_positions
),
filtered_position_changes AS (
  SELECT pc.*
  FROM position_changes pc
  WHERE pc.lap > 1 -- exclude first lap
),
non_pit_retire_changes AS (
  SELECT fpc.*
  FROM filtered_position_changes fpc
  LEFT JOIN pit_stops ps ON fpc.race_id = ps.race_id AND fpc.driver_id = ps.driver_id AND fpc.lap = ps.lap
  LEFT JOIN retirements rt ON fpc.race_id = rt.race_id AND fpc.driver_id = rt.driver_id AND fpc.lap = rt.lap
  WHERE ps.lap IS NULL AND rt.lap IS NULL
),
pos_changes AS (
  SELECT 
    driver_id,
    SUM(CASE WHEN previous_position > current_position THEN 1 ELSE 0 END) AS overtaken_others,
    SUM(CASE WHEN previous_position < current_position THEN 1 ELSE 0 END) AS overtaken_by_others
  FROM non_pit_retire_changes
  WHERE previous_position IS NOT NULL
  GROUP BY driver_id
),
drivers_overtaken_more AS (
  SELECT driver_id
  FROM pos_changes
  WHERE overtaken_by_others > overtaken_others
)
SELECT DISTINCT d.forename || ' ' || d.surname AS full_name
FROM drivers d
JOIN drivers_overtaken_more dom ON d.driver_id = dom.driver_id
ORDER BY full_name;
