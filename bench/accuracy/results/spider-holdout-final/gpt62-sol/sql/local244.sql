WITH duration_bounds AS (
    SELECT (MIN(Milliseconds) + AVG(Milliseconds)) / 2.0 AS short_max_ms,
           (AVG(Milliseconds) + MAX(Milliseconds)) / 2.0 AS medium_max_ms
    FROM Track
), classified_tracks AS (
    SELECT t.TrackId,
           t.Milliseconds / 60000.0 AS duration_minutes,
           CASE
               WHEN t.Milliseconds <= b.short_max_ms THEN 'Short'
               WHEN t.Milliseconds <= b.medium_max_ms THEN 'Medium'
               ELSE 'Long'
           END AS category
    FROM Track AS t
    CROSS JOIN duration_bounds AS b
), track_revenue AS (
    SELECT TrackId, SUM(UnitPrice * Quantity) AS revenue
    FROM InvoiceLine
    GROUP BY TrackId
)
SELECT c.category,
       MIN(c.duration_minutes) AS min_minutes,
       MAX(c.duration_minutes) AS max_minutes,
       ROUND(SUM(r.revenue), 2) AS total_revenue
FROM classified_tracks AS c
LEFT JOIN track_revenue AS r ON r.TrackId = c.TrackId
GROUP BY c.category
ORDER BY min_minutes;
