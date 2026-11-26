WITH earliest_edition AS (
    SELECT raceId, year
    FROM races
    WHERE name = 'Chinese Grand Prix'
      AND year BETWEEN 2000 AND 2009
      AND TIME(time) IS NOT NULL
    ORDER BY TIME(time) ASC, year DESC
    LIMIT 1
)
SELECT e.year AS season_year, r.time AS runner_up_time_gap
FROM earliest_edition AS e
JOIN results AS r ON r.raceId = e.raceId
WHERE r.position = 2;
