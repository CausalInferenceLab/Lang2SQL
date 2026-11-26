SELECT ROUND(
    100.0 * COUNT(DISTINCT CASE
        WHEN s.status = 'Finished' OR s.status LIKE '+% Lap%' THEN re.driverId
    END) / COUNT(DISTINCT re.driverId),
    2
) AS classified_finisher_percentage
FROM races AS r
JOIN results AS re ON re.raceId = r.raceId
JOIN status AS s ON s.statusId = re.statusId
WHERE r.year = 1957
  AND r.name = 'British Grand Prix'
  AND re.positionText <> 'W';
