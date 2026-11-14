WITH ordered_visits AS (
  SELECT
    session,
    stamp,
    path,
    LEAD(path, 1) OVER (PARTITION BY session ORDER BY stamp) AS next_page1,
    LEAD(path, 2) OVER (PARTITION BY session ORDER BY stamp) AS next_page2
  FROM activity_log
),

consecutive_detail_third_page AS (
  SELECT
    next_page2 AS third_page
  FROM ordered_visits
  WHERE path = '/detail'
    AND next_page1 = '/detail'
    AND next_page2 IS NOT NULL
)

SELECT
  third_page,
  COUNT(*) AS visit_count
FROM consecutive_detail_third_page
GROUP BY third_page
ORDER BY visit_count DESC
LIMIT 3;
