WITH first_target AS (
    SELECT session, MIN(stamp) AS first_stamp
    FROM activity_log
    WHERE path IN ('/detail', '/complete')
    GROUP BY session
), pre_target_counts AS (
    SELECT f.session, f.first_stamp, COUNT(e.stamp) AS event_count
    FROM first_target AS f
    LEFT JOIN activity_log AS e
        ON e.session = f.session
       AND e.stamp < f.first_stamp
       AND e.search_type <> ''
    GROUP BY f.session, f.first_stamp
)
SELECT DISTINCT c.session, t.path, t.search_type
FROM pre_target_counts AS c
JOIN activity_log AS t
    ON t.session = c.session
   AND t.stamp = c.first_stamp
   AND t.path IN ('/detail', '/complete')
WHERE c.event_count = (SELECT MIN(event_count) FROM pre_target_counts)
ORDER BY c.session, t.path, t.search_type;
