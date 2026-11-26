WITH local_starts AS (
    SELECT c.circuitRef,
           time(r.time, printf('%+d minutes', CASE
               -- These archive entries already use venue-local time.
               WHEN r.year <= 2006
                 OR (c.circuitRef = 'yas_marina' AND r.date = '2017-11-26') THEN 0
               -- UTC offsets on the recorded race dates.
               WHEN c.circuitRef = 'americas' THEN
                   CASE WHEN r.date < date(r.year || '-11-01', 'weekday 0')
                        THEN -300 ELSE -360 END
               WHEN c.circuitRef = 'rodriguez' THEN -360
               WHEN c.circuitRef IN ('indianapolis', 'villeneuve') THEN -240
               WHEN c.circuitRef = 'interlagos' THEN -120
               WHEN c.circuitRef = 'silverstone' THEN 60
               WHEN c.circuitRef IN ('bahrain', 'istanbul') THEN 180
               WHEN c.circuitRef IN ('BAK', 'yas_marina') THEN 240
               WHEN c.circuitRef = 'sochi' THEN CASE WHEN r.year = 2014 THEN 240 ELSE 180 END
               WHEN c.circuitRef = 'buddh' THEN 330
               WHEN c.circuitRef IN ('marina_bay', 'sepang', 'shanghai') THEN 480
               WHEN c.circuitRef IN ('fuji', 'suzuka', 'yeongam') THEN 540
               WHEN c.circuitRef IN ('catalunya', 'hockenheimring', 'hungaroring',
                                     'magny_cours', 'monaco', 'monza', 'nurburgring',
                                     'red_bull_ring', 'spa', 'valencia') THEN 120
           END)) AS local_start
    FROM races AS r
    JOIN circuits AS c ON c.circuitId = r.circuitId
    WHERE r.time IS NOT NULL
), extrema AS (
    SELECT MIN(local_start) AS earliest, MAX(local_start) AS latest
    FROM local_starts
)
SELECT (SELECT group_concat(circuitRef, ', ')
        FROM (SELECT DISTINCT circuitRef FROM local_starts
              WHERE local_start = e.earliest ORDER BY circuitRef)) AS earliest_circuit_refs,
       (SELECT group_concat(circuitRef, ', ')
        FROM (SELECT DISTINCT circuitRef FROM local_starts
              WHERE local_start = e.latest ORDER BY circuitRef)) AS latest_circuit_refs,
       CAST(strftime('%s', e.latest) AS INTEGER)
       - CAST(strftime('%s', e.earliest) AS INTEGER) AS gap_seconds
FROM extrema AS e;
