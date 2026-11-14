WITH ordered_visits AS (
  SELECT
    session,
    action AS page_path,
    ROW_NUMBER() OVER (PARTITION BY session ORDER BY stamp) AS visit_order
  FROM access_log
  WHERE action IN ('/regist/input', '/regist/confirm')
),
seq_sessions AS (
  SELECT
    a.session
  FROM ordered_visits a
  JOIN ordered_visits b ON a.session = b.session
  WHERE a.page_path = '/regist/input'
    AND b.page_path = '/regist/confirm'
    AND a.visit_order < b.visit_order
)
SELECT COUNT(DISTINCT session) AS unique_sessions
FROM seq_sessions;
