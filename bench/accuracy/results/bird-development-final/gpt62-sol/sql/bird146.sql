WITH RECURSIVE months(month_start) AS (
    SELECT '2010-01-01'
    UNION ALL
    SELECT date(month_start, '+1 month')
    FROM months
    WHERE month_start < '2010-12-01'
), monthly_counts AS (
    SELECT date(pl.CreationDate, 'start of month') AS month_start,
           COUNT(DISTINCT p.Id) AS post_count
    FROM postLinks AS pl
    JOIN posts AS p ON p.Id = pl.RelatedPostId
    WHERE pl.CreationDate >= '2010-01-01'
      AND pl.CreationDate < '2011-01-01'
      AND p.AnswerCount <= 2
    GROUP BY date(pl.CreationDate, 'start of month')
)
SELECT ROUND(AVG(COALESCE(mc.post_count, 0)), 2) AS average_monthly_posts
FROM months AS m
LEFT JOIN monthly_counts AS mc ON mc.month_start = m.month_start;
