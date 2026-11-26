WITH monthly_counts AS (
    SELECT strftime('%Y-%m', l.CreationDate) AS month,
           COUNT(DISTINCT l.RelatedPostId) AS post_count
    FROM main.postLinks AS l
    JOIN main.posts AS p ON p.Id = l.RelatedPostId
    WHERE l.CreationDate >= '2010-01-01'
      AND l.CreationDate < '2011-01-01'
      AND p.AnswerCount <= 2
    GROUP BY month
)
SELECT ROUND(COALESCE(SUM(post_count), 0) / 12.0, 2) AS average_monthly_posts
FROM monthly_counts
LIMIT 1000
