WITH overtakes AS (
  SELECT
    lp1.raceId,
    lp1.lap AS overtake_lap,
    lp1.driverId AS overtaker_id,
    lp2.driverId AS overtaken_id,
    lp1.position AS overtaker_pos,
    lp2.position AS overtaken_pos,
    lp1.lap,
    d1.grid AS overtaker_grid,
    d2.grid AS overtaken_grid,
    -- Check if overtaken retired on this lap
    CASE WHEN r.driverId IS NOT NULL AND r.lap = lp1.lap THEN 1 ELSE 0 END AS is_retirement,
    -- Did overtaken pit on this lap?
    CASE WHEN p.driverId IS NOT NULL AND p.lap = lp1.lap THEN 1 ELSE 0 END AS pitted_this_lap,
    -- Did overtaken pit on previous lap?
    CASE WHEN prev_pit.driverId IS NOT NULL AND prev_pit.lap = lp1.lap - 1 THEN 1 ELSE 0 END AS pitted_prev_lap,
    -- Pit-in or pit-out case for pit overtakes
    p.stop AS pit_stop_number,
    prev_pit.stop AS prev_pit_stop_number
  FROM lap_positions lp1
  JOIN lap_positions lp2
    ON lp1.raceId = lp2.raceId
    AND lp1.lap = lp2.lap
    AND lp1.position = lp2.position - 1
  JOIN drivers d1 ON lp1.driverId = d1.driverId
  JOIN drivers d2 ON lp2.driverId = d2.driverId
  LEFT JOIN retirements r ON lp2.raceId = r.raceId AND lp2.driverId = r.driverId AND r.lap = lp1.lap
  LEFT JOIN pit_stops p ON lp2.raceId = p.raceId AND lp2.driverId = p.driverId AND p.lap = lp1.lap
  LEFT JOIN pit_stops prev_pit ON lp2.raceId = prev_pit.raceId AND lp2.driverId = prev_pit.driverId AND prev_pit.lap = lp1.lap - 1
  WHERE lp1.lap BETWEEN 1 AND 5
),
overtake_labels AS (
  SELECT
    raceId,
    overtake_lap,
    overtaker_id,
    overtaken_id,
    overtaker_pos,
    overtaken_pos,
    overtaker_grid,
    overtaken_grid,
    CASE
      -- 1. Retirement: overtaken retired on this lap
      WHEN is_retirement = 1 THEN 'Retirement'
      -- 2. Pit related: pitted this lap (entry), or pitted previous lap and not this lap (exit)
      WHEN pitted_this_lap = 1 THEN 'Pit'
      WHEN pitted_prev_lap = 1 AND pitted_this_lap = 0 THEN 'Pit'
      -- 3. Start: First lap and within 2 grid positions at start
      WHEN overtake_lap = 1 AND ABS(overtaker_grid - overtaken_grid) <= 2 THEN 'Start'
      -- 4. Track: All other cases
      ELSE 'Track'
    END AS overtake_category
  FROM overtakes
)
SELECT
  overtake_category,
  COUNT(*) AS overtake_count
FROM overtake_labels
GROUP BY overtake_category
ORDER BY
  CASE overtake_category
    WHEN 'Retirement' THEN 1
    WHEN 'Pit' THEN 2
    WHEN 'Start' THEN 3
    WHEN 'Track' THEN 4
    ELSE 5
  END;
