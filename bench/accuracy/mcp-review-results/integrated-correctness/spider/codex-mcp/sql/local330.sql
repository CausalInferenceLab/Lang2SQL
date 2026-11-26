WITH page_visits AS (
    SELECT session,
           path,
           stamp,
           MIN(stamp) OVER (PARTITION BY session) AS first_stamp,
           MAX(stamp) OVER (PARTITION BY session) AS last_stamp
    FROM main.activity_log
    WHERE action = 'view'
)
SELECT path,
       COUNT(DISTINCT CASE
           WHEN stamp = first_stamp OR stamp = last_stamp THEN session
       END) AS session_count
FROM page_visits
WHERE path IS NOT NULL AND path <> ''
GROUP BY path
ORDER BY path
LIMIT -1
