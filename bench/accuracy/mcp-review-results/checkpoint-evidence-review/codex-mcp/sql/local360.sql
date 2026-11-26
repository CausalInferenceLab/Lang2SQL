WITH events AS (
    SELECT session, stamp, path, search_type,
           MIN(CASE WHEN path IN ('/detail', '/complete') THEN stamp END)
               OVER (PARTITION BY session) AS first_target
    FROM main.activity_log
),
session_counts AS (
    SELECT session, first_target,
           COUNT(CASE
               WHEN search_type <> '' AND (first_target IS NULL OR stamp < first_target)
               THEN 1
           END) AS event_count
    FROM events
    GROUP BY session, first_target
)
SELECT DISTINCT c.session, c.event_count, e.path, e.search_type
FROM session_counts AS c
LEFT JOIN events AS e
    ON e.session = c.session
   AND e.stamp = c.first_target
   AND e.path IN ('/detail', '/complete')
WHERE c.event_count = (SELECT MIN(event_count) FROM session_counts)
ORDER BY c.session, e.path, e.search_type
LIMIT 1000
