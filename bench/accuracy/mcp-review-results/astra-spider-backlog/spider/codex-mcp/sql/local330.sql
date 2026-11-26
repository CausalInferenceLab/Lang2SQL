WITH page_visits AS (
    SELECT
        path,
        session,
        stamp,
        MIN(stamp) OVER (PARTITION BY session) AS first_stamp,
        MAX(stamp) OVER (PARTITION BY session) AS last_stamp
    FROM main.activity_log
    WHERE action = 'view'
)
SELECT
    path,
    COUNT(DISTINCT session) FILTER (
        WHERE stamp = first_stamp OR stamp = last_stamp
    ) AS session_count
FROM page_visits
GROUP BY path
ORDER BY path
LIMIT 1000
