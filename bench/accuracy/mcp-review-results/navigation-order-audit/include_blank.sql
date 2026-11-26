WITH visits AS (
    SELECT
        path AS third_page,
        LAG(path, 1) OVER visit_order AS previous_page,
        LAG(path, 2) OVER visit_order AS two_pages_back
    FROM main.activity_log
    WHERE action = 'view'
    WINDOW visit_order AS (PARTITION BY session ORDER BY stamp, rowid)
)
SELECT third_page, COUNT(*) AS occurrences
FROM visits
WHERE previous_page = '/detail'
  AND two_pages_back = '/detail'
  
GROUP BY third_page
ORDER BY occurrences DESC, third_page
LIMIT 3;
