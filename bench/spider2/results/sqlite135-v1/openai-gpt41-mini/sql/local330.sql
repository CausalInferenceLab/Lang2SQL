WITH session_ordered AS (
  SELECT
    session,
    path,
    stamp,
    ROW_NUMBER() OVER (PARTITION BY session ORDER BY stamp ASC) AS rn_asc,
    ROW_NUMBER() OVER (PARTITION BY session ORDER BY stamp DESC) AS rn_desc
  FROM activity_log
), landing_pages AS (
  SELECT session, path AS landing_page
  FROM session_ordered
  WHERE rn_asc = 1
), exit_pages AS (
  SELECT session, path AS exit_page
  FROM session_ordered
  WHERE rn_desc = 1
), landing_exit_sessions AS (
  SELECT landing_page AS page, session FROM landing_pages
  UNION
  SELECT exit_page AS page, session FROM exit_pages
)
SELECT page, COUNT(DISTINCT session) AS unique_sessions
FROM landing_exit_sessions
GROUP BY page
ORDER BY unique_sessions DESC;
