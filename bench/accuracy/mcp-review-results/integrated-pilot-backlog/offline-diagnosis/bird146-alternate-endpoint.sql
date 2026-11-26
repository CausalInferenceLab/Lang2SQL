WITH RECURSIVE months(month_start) AS (
    SELECT '2010-01-01'
    UNION ALL
    SELECT date(month_start, '+1 month')
    FROM months
    WHERE month_start < '2010-12-01'
),
monthly_counts AS (
    SELECT
        date(l.CreationDate, 'start of month') AS month_start,
        COUNT(DISTINCT p.Id) AS post_count
    FROM main.postLinks AS l
    JOIN main.posts AS p ON p.Id = l.PostId
    WHERE l.CreationDate >= '2010-01-01'
      AND l.CreationDate < '2011-01-01'
      AND p.AnswerCount <= 2
    GROUP BY date(l.CreationDate, 'start of month')
)
SELECT ROUND(AVG(COALESCE(c.post_count, 0)), 2) AS average_monthly_posts
FROM months AS m
LEFT JOIN monthly_counts AS c ON c.month_start = m.month_start
LIMIT 1
