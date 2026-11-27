WITH winner_times AS (
    SELECT r.year, r.raceId, MIN(res.milliseconds) AS elapsed_ms
    FROM races AS r
    JOIN results AS res ON res.raceId = r.raceId
    WHERE r.year < 1975
      AND res.position = 1
      AND res.milliseconds IS NOT NULL
    GROUP BY r.year, r.raceId
)
SELECT year,
       ((SUM(elapsed_ms) + 5 * COUNT(*)) / (10 * COUNT(*))) / 100.0
           AS mean_winner_elapsed_seconds
FROM winner_times
GROUP BY year
ORDER BY year;
