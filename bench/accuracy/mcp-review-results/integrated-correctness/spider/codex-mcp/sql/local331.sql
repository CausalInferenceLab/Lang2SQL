WITH visits AS (
    SELECT path,
           LEAD(path, 1) OVER visit_order AS second_page,
           LEAD(path, 2) OVER visit_order AS third_page
    FROM main.activity_log
    WHERE action = 'view'
    WINDOW visit_order AS (PARTITION BY session ORDER BY stamp, rowid)
)
SELECT third_page, COUNT(*) AS visit_count
FROM visits
WHERE path = '/detail'
  AND second_page = '/detail'
  AND third_page IS NOT NULL
GROUP BY third_page
ORDER BY visit_count DESC, third_page DESC
LIMIT 3
