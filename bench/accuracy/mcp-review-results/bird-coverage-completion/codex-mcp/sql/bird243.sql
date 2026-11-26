SELECT ROUND(
    100.0 * (finisher.milliseconds - winner.milliseconds)
    / NULLIF(finisher.milliseconds, 0), 2
) AS percentage_shorter
FROM races AS gp
JOIN results AS winner
  ON winner.raceId = gp.raceId AND winner.position = 1
JOIN results AS finisher
  ON finisher.raceId = gp.raceId AND finisher.laps = winner.laps
JOIN status AS s ON s.statusId = finisher.statusId
WHERE gp.year = 1988
  AND gp.name = 'Australian Grand Prix'
  AND s.status = 'Finished'
ORDER BY finisher.positionOrder DESC
LIMIT 1
