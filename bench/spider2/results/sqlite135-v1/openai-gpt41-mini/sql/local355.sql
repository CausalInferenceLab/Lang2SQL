WITH driver_year_missed_races AS (
    SELECT
        d.driver_id,
        r.year,
        MIN(r.round) AS first_missed_round,
        MAX(r.round) AS last_missed_round,
        COUNT(*) AS missed_count
    FROM results res
    JOIN races r ON res.race_id = r.race_id
    JOIN drivers d ON res.driver_id = d.driver_id
    WHERE res.position IS NULL
    GROUP BY d.driver_id, r.year
    HAVING COUNT(*) < 3
),
drivers_switched_around_misses AS (
    SELECT
        dymr.driver_id,
        dymr.year,
        dymr.first_missed_round,
        dymr.last_missed_round
    FROM driver_year_missed_races dymr
    JOIN races r_before ON r_before.year = dymr.year AND r_before.round = dymr.first_missed_round - 1
    JOIN races r_after ON r_after.year = dymr.year AND r_after.round = dymr.last_missed_round + 1
    JOIN results res_before ON res_before.driver_id = dymr.driver_id AND res_before.race_id = r_before.race_id
    JOIN results res_after ON res_after.driver_id = dymr.driver_id AND res_after.race_id = r_after.race_id
    WHERE res_before.constructor_id <> res_after.constructor_id
),
averages AS (
    SELECT
        AVG(first_missed_round) AS avg_first_missed_round,
        AVG(last_missed_round) AS avg_last_missed_round
    FROM drivers_switched_around_misses
)
SELECT avg_first_missed_round, avg_last_missed_round FROM averages;
