WITH first_action AS (
  SELECT
    session,
    MIN(CASE 
      WHEN path = '/detail' OR path = '/complete' THEN stamp 
      ELSE NULL 
    END) AS first_click_or_conversion_time
  FROM activity_log
  GROUP BY session
),

count_pre_events AS (
  SELECT
    a.session,
    COUNT(*) AS pre_event_count
  FROM activity_log a
  JOIN first_action f ON a.session = f.session
  WHERE a.stamp < f.first_click_or_conversion_time
    AND a.search_type IS NOT NULL
    AND TRIM(a.search_type) != ''
  GROUP BY a.session
),

min_count AS (
  SELECT MIN(pre_event_count) AS min_pre_event_count FROM count_pre_events
),

sessions_min_count AS (
  SELECT session FROM count_pre_events c
  JOIN min_count m ON c.pre_event_count = m.min_pre_event_count
)

SELECT a.session, a.path, a.search_type
FROM activity_log a
JOIN sessions_min_count s ON a.session = s.session
WHERE a.search_type IS NOT NULL
  AND TRIM(a.search_type) != ''
ORDER BY a.session, a.stamp
LIMIT 1000
;
