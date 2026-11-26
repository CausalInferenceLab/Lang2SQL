WITH session_cutoffs AS (
    SELECT session,
           MIN(CASE WHEN path IN ('/detail', '/complete') THEN stamp END) AS first_target
    FROM main.activity_log
    GROUP BY session
),
pre_target_events AS (
    SELECT c.session, e.path, e.search_type
    FROM session_cutoffs AS c
    LEFT JOIN main.activity_log AS e
      ON e.session = c.session
     AND e.search_type <> ''
     AND (c.first_target IS NULL OR e.stamp < c.first_target)
    WHERE c.first_target IS NOT NULL
),
session_counts AS (
    SELECT session, COUNT(search_type) AS event_count
    FROM pre_target_events
    GROUP BY session
)
SELECT DISTINCT c.session, c.event_count, e.path, e.search_type
FROM session_counts AS c
JOIN session_cutoffs AS cutoff ON cutoff.session = c.session
JOIN main.activity_log AS e ON e.session = c.session AND e.stamp = cutoff.first_target AND e.path IN ('/detail', '/complete')
WHERE c.event_count = (SELECT MIN(event_count) FROM session_counts)
ORDER BY c.session, e.path, e.search_type
LIMIT 1000
