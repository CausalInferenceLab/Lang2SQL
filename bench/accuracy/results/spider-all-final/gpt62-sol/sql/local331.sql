WITH visits AS (
    SELECT
        path,
        LAG(path, 1) OVER visit_order AS previous_path,
        LAG(path, 2) OVER visit_order AS second_previous_path
    FROM activity_log
    WHERE action = 'view'
    WINDOW visit_order AS (PARTITION BY session ORDER BY stamp, rowid)
)
SELECT path AS third_page, COUNT(*) AS visit_count
FROM visits
WHERE previous_path = '/detail'
  AND second_previous_path = '/detail'
GROUP BY path
ORDER BY visit_count DESC, third_page ASC
LIMIT 3;
