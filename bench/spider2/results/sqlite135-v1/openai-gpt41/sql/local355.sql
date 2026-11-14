WITH driver_race_participation AS (
    SELECT
        d.driverId,
        r.year,
        r.raceId,
        r.round,
        res.constructorId
    FROM drivers d
    JOIN results res ON d.driverId = res.driverId
    JOIN races r ON res.raceId = r.raceId
),
driver_year_rounds AS (
    SELECT
        driverId,
        year,
        round,
        constructorId
    FROM driver_race_participation
),
driver_year_stats AS (
    SELECT
        driverId,
        year,
        MIN(round) AS first_round,
        MAX(round) AS last_round
    FROM driver_year_rounds
    GROUP BY driverId, year
),
all_rounds_per_year AS (
    SELECT year, MIN(round) as min_round, MAX(round) as max_round
    FROM races
    GROUP BY year
),
driver_full_year_template AS (
    SELECT
        dy.driverId,
        dy.year,
        arpy.round
    FROM driver_year_stats dy
    JOIN all_rounds_per_year arpy ON dy.year = arpy.year
    WHERE arpy.round BETWEEN dy.first_round AND dy.last_round
),
driver_year_appearance_flags AS (
    SELECT
        dfyt.driverId,
        dfyt.year,
        dfyt.round,
        CASE WHEN dyr.round IS NOT NULL THEN 1 ELSE 0 END AS participated,
        dyr.constructorId
    FROM driver_full_year_template dfyt
    LEFT JOIN driver_year_rounds dyr
        ON dfyt.driverId = dyr.driverId AND dfyt.year = dyr.year AND dfyt.round = dyr.round
),
driver_missed_hiatuses AS (
    SELECT
        driverId,
        year,
        group_num,
        MIN(round) AS first_missed_round,
        MAX(round) AS last_missed_round
    FROM (
        SELECT
            *,
            SUM(CASE WHEN participated = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY driverId, year ORDER BY round) 
                - ROW_NUMBER() OVER (PARTITION BY driverId, year ORDER BY round) AS group_num
        FROM driver_year_appearance_flags
    )
    WHERE participated = 0
    GROUP BY driverId, year, group_num
),
count_hiatuses AS (
    SELECT
        driverId,
        year,
        COUNT(*) AS total_hiatuses
    FROM driver_missed_hiatuses
    GROUP BY driverId, year
),
filtered_hiatuses AS (
    SELECT
        mh.*,
        daf_before.constructorId as before_missed_cid,
        daf_after.constructorId as after_missed_cid
    FROM driver_missed_hiatuses mh
    JOIN driver_year_appearance_flags daf_before ON
        mh.driverId = daf_before.driverId AND mh.year = daf_before.year AND daf_before.round = mh.first_missed_round - 1 AND daf_before.participated = 1
    JOIN driver_year_appearance_flags daf_after ON
        mh.driverId = daf_after.driverId AND mh.year = daf_after.year AND daf_after.round = mh.last_missed_round + 1 AND daf_after.participated = 1
    JOIN count_hiatuses ch ON
        mh.driverId = ch.driverId AND mh.year = ch.year
    WHERE (mh.last_missed_round - mh.first_missed_round + 1) < 3
      AND daf_before.constructorId IS NOT NULL
      AND daf_after.constructorId IS NOT NULL
      AND daf_before.constructorId != daf_after.constructorId
      AND ch.total_hiatuses = (
          SELECT COUNT(*)
          FROM driver_missed_hiatuses i
          WHERE i.driverId = mh.driverId AND i.year = mh.year
      )
),
summary AS (
    SELECT
        AVG(first_missed_round * 1.0) AS avg_first_missed_round,
        AVG(last_missed_round * 1.0) AS avg_last_missed_round
    FROM filtered_hiatuses
)
SELECT
    avg_first_missed_round,
    avg_last_missed_round
FROM summary;
