WITH bounds AS (
    SELECT session, MIN(stamp) AS first_stamp, MAX(stamp) AS last_stamp
    FROM activity_log
    GROUP BY session
), endpoints AS (
    SELECT DISTINCT a.session, a.path
    FROM activity_log AS a
    JOIN bounds AS b ON a.session = b.session
    WHERE a.stamp = b.first_stamp OR a.stamp = b.last_stamp
)
SELECT p.path, COUNT(e.session) AS unique_session_count
FROM (SELECT DISTINCT path FROM activity_log) AS p
LEFT JOIN endpoints AS e ON e.path = p.path
GROUP BY p.path
ORDER BY unique_session_count DESC, p.path;
