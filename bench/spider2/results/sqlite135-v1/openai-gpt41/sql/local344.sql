WITH lap_gaps AS (
    SELECT
        lt1.raceId,
        lt1.driverId AS overtaker_id,
        lt1.lap AS curr_lap,
        lt2.driverId AS overtaken_id,
        lt1.position AS overtaker_pos,
        lt2.position AS overtaken_pos,
        lt_prev.position AS overtaker_prev_pos,
        lt2_prev.position AS overtaken_prev_pos
    FROM lap_times lt1
    JOIN lap_times lt2
        ON lt1.raceId = lt2.raceId
        AND lt1.lap = lt2.lap
        AND lt1.driverId != lt2.driverId
    LEFT JOIN lap_times lt_prev
        ON lt_prev.raceId = lt1.raceId
        AND lt_prev.lap = lt1.lap - 1
        AND lt_prev.driverId = lt1.driverId
    LEFT JOIN lap_times lt2_prev
        ON lt2_prev.raceId = lt2.raceId
        AND lt2_prev.lap = lt2.lap - 1
        AND lt2_prev.driverId = lt2.driverId
    WHERE lt1.position = lt2.position + 1
      AND (lt_prev.position IS NULL OR lt_prev.position != lt2_prev.position + 1)
), pit_stops_window AS (
    SELECT
        ps.raceId,
        ps.driverId,
        ps.lap AS pit_lap,
        ps.stop
    FROM pit_stops ps
), overtaken_retirements AS (
    SELECT
        r.raceId,
        r.driverId,
        r.lap
    FROM retirements r
), grid AS (
    SELECT
        q.raceId,
        q.driverId,
        q.grid
    FROM results q
), overtake_type AS (
    SELECT
        lg.*,
        r.lap IS NOT NULL AS overtaken_retired,
        ps_entry_stop.stop AS pit_entry_stop,
        ps_exit_stop.stop AS pit_exit_stop,
        COALESCE(g1.grid, 0) AS overtaker_grid,
        COALESCE(g2.grid, 0) AS overtaken_grid
    FROM lap_gaps lg
    LEFT JOIN overtaken_retirements r
        ON lg.raceId = r.raceId AND lg.overtaken_id = r.driverId AND lg.curr_lap = r.lap
    LEFT JOIN pit_stops_window ps_entry_stop
        ON lg.raceId = ps_entry_stop.raceId
        AND lg.overtaken_id = ps_entry_stop.driverId
        AND lg.curr_lap = ps_entry_stop.pit_lap
    LEFT JOIN pit_stops_window ps_exit_stop
        ON lg.raceId = ps_exit_stop.raceId
        AND lg.overtaken_id = ps_exit_stop.driverId
        AND lg.curr_lap - 1 = ps_exit_stop.pit_lap
    LEFT JOIN grid g1
        ON lg.raceId = g1.raceId AND lg.overtaker_id = g1.driverId
    LEFT JOIN grid g2
        ON lg.raceId = g2.raceId AND lg.overtaken_id = g2.driverId
)
SELECT
    CASE
        WHEN overtaken_retired THEN 'R'
        WHEN pit_entry_stop IS NOT NULL THEN 'P'
        WHEN pit_exit_stop IS NOT NULL THEN 'P'
        WHEN curr_lap = 1 AND ABS(overtaker_grid - overtaken_grid) <= 2 THEN 'S'
        ELSE 'T'
    END AS overtake_label,
    COUNT(*) AS occurrences
FROM overtake_type
GROUP BY overtake_label
ORDER BY overtake_label;
