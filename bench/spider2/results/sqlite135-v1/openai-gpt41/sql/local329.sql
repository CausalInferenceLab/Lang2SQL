WITH ordered_visits AS (
  SELECT
    session_id,
    path,
    timestamp,
    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY timestamp) AS rn
  FROM access_log
  WHERE path IN ('/regist/input', '/regist/confirm')
),
input_times AS (
  SELECT
    session_id,
    MIN(timestamp) AS input_time
  FROM ordered_visits
  WHERE path = '/regist/input'
  GROUP BY session_id
),
confirm_times AS (
  SELECT
    session_id,
    MIN(timestamp) AS confirm_time
  FROM ordered_visits
  WHERE path = '/regist/confirm'
  GROUP BY session_id
)
SELECT COUNT(DISTINCT i.session_id) AS unique_sessions
FROM input_times i
JOIN confirm_times c
  ON i.session_id = c.session_id
  AND i.input_time < c.confirm_time;
