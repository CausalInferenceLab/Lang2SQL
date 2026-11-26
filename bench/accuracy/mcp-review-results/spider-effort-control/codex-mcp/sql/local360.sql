WITH sessions AS (
    SELECT session,
           MIN(CASE WHEN path IN ('/detail', '/complete') THEN stamp END) AS first_target
    FROM main.activity_log
    GROUP BY session
),
counted_events AS (
    SELECT s.session, e.path, e.search_type,
           COUNT(e.search_type) OVER (PARTITION BY s.session) AS event_count
    FROM sessions AS s
    LEFT JOIN main.activity_log AS e
      ON e.session = s.session
     AND e.search_type <> ''
     AND (s.first_target IS NULL OR e.stamp < s.first_target)
)
SELECT DISTINCT session, path, search_type, event_count
FROM counted_events
WHERE event_count = (SELECT MIN(event_count) FROM counted_events)
ORDER BY session, path, search_type
LIMIT 1000
