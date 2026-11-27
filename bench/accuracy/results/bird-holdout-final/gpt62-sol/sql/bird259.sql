SELECT d.forename,
       d.surname,
       strftime('%H:%M:%f', r.date || ' ' || r.time,
                '+4 hours', '+' || (res.milliseconds / 1000.0) || ' seconds') AS local_finish_time,
       d.url
FROM races AS r
JOIN results AS res ON res.raceId = r.raceId
JOIN drivers AS d ON d.driverId = res.driverId
WHERE r.year = 2017
  AND r.name = 'Azerbaijan Grand Prix'
  AND res.position = 1;
