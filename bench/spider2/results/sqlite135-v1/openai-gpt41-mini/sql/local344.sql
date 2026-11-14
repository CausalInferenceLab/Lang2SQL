WITH valid_races AS (
  SELECT DISTINCT race_id
  FROM pit_stops
),
lap_pos AS (
  SELECT
    race_id,
    driver_id,
    lap,
    position,
    LAG(position) OVER (PARTITION BY race_id, driver_id ORDER BY lap) AS position_prev
  FROM lap_positions
  WHERE race_id IN (SELECT race_id FROM valid_races)
),
possible_overtakes AS (
  SELECT
    current.race_id,
    current.driver_id AS overtaking_driver,
    competitors.driver_id AS overtaken_driver,
    current.lap,
    current.position_prev AS overtaking_prev_pos,
    current.position AS overtaking_pos,
    competitors.position_prev AS overtaken_prev_pos,
    competitors.position AS overtaken_pos
  FROM lap_pos current
  JOIN lap_pos competitors ON current.race_id = competitors.race_id AND current.lap = competitors.lap
  WHERE 
    current.position_prev IS NOT NULL AND competitors.position_prev IS NOT NULL
    AND current.position_prev < current.position
    AND current.position_prev < competitors.position_prev
    AND current.position > competitors.position
),
retired AS (
  SELECT race_id, driver_id, lap FROM retirements
),
pit_entry AS (
  SELECT race_id, driver_id, lap FROM pit_stops
),
start_grid AS (
  SELECT race_id, driver_id, position AS grid_position FROM qualifying
),
classified_overtakes AS (
  SELECT 
    p.race_id, p.lap, p.overtaking_driver, p.overtaken_driver,
    CASE
      WHEN r.driver_id IS NOT NULL AND r.lap = p.lap THEN 'R'
      WHEN pe.driver_id IS NOT NULL AND pe.lap = p.lap THEN 'P'
      WHEN po.driver_id IS NOT NULL AND po.lap = p.lap - 1 THEN 'P'
      WHEN p.lap = 1 AND ABS(s_o.grid_position - s_t.grid_position) <= 2 THEN 'S'
      ELSE 'T'
    END AS overtake_type
  FROM possible_overtakes p
  LEFT JOIN retired r ON p.race_id = r.race_id AND p.overtaken_driver = r.driver_id AND p.lap = r.lap
  LEFT JOIN pit_entry pe ON p.race_id = pe.race_id AND p.overtaken_driver = pe.driver_id AND p.lap = pe.lap
  LEFT JOIN pit_entry po ON p.race_id = po.race_id AND p.overtaken_driver = po.driver_id AND po.lap = p.lap - 1
  LEFT JOIN start_grid s_o ON p.race_id = s_o.race_id AND p.overtaking_driver = s_o.driver_id
  LEFT JOIN start_grid s_t ON p.race_id = s_t.race_id AND p.overtaken_driver = s_t.driver_id
)
SELECT overtake_type, COUNT(*) AS overtake_count
FROM classified_overtakes
GROUP BY overtake_type
ORDER BY overtake_type;
