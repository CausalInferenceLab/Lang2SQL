WITH event_cte AS (
  SELECT
    session_id,
    event_time,
    path,
    search_type,
    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time) AS rn
  FROM activity_log
  WHERE search_type IS NOT NULL AND search_type != ''
),
first_target_event AS (
  SELECT
    session_id,
    MIN(event_time) AS first_target_time
  FROM activity_log
  WHERE path IN ('/detail', '/complete')
  GROUP BY session_id
),
marked_events AS (
  SELECT
    e.session_id,
    e.path,
    e.search_type,
    e.event_time,
    COALESCE(fte.first_target_time, '9999-12-31') AS first_target_time
  FROM event_cte e
  LEFT JOIN first_target_event fte
    ON e.session_id = fte.session_id
),
pre_click_counts AS (
  SELECT
    session_id,
    COUNT(*) AS pre_event_count
  FROM marked_events
  WHERE event_time < first_target_time
  GROUP BY session_id
),
min_count AS (
  SELECT MIN(pre_event_count) AS min_pre_event_count
  FROM pre_click_counts
),
qual_sessions AS (
  SELECT
    pcc.session_id,
    pcc.pre_event_count
  FROM pre_click_counts pcc
  JOIN min_count mc ON pcc.pre_event_count = mc.min_pre_event_count
)
SELECT
  me.session_id,
  me.path,
  me.search_type,
  qs.pre_event_count AS min_pre_event_count
FROM marked_events me
JOIN qual_sessions qs ON me.session_id = qs.session_id
WHERE me.event_time < me.first_target_time
ORDER BY me.session_id, me.event_time;
