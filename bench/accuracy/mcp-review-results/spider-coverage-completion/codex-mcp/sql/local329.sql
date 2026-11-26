WITH session_visits AS (
    SELECT
        session,
        MIN(CASE WHEN path = '/regist/input' THEN stamp END) AS first_input,
        MAX(CASE WHEN path = '/regist/confirm' THEN stamp END) AS last_confirm
    FROM main.form_log
    WHERE action = 'view'
      AND path IN ('/regist/input', '/regist/confirm')
    GROUP BY session
)
SELECT COUNT(session) AS unique_sessions
FROM session_visits
WHERE first_input < last_confirm
LIMIT 1
