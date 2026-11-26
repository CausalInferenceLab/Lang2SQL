WITH monthly_counts AS (
    SELECT COUNT(DISTINCT l.RelatedPostId) AS receiving_posts
    FROM main.postLinks AS l
    JOIN main.posts AS p ON p.Id = l.RelatedPostId
    WHERE l.CreationDate >= '2010-01-01'
      AND l.CreationDate < '2011-01-01'
      AND p.AnswerCount <= 2
    GROUP BY strftime('%Y-%m', l.CreationDate)
)
SELECT ROUND(TOTAL(receiving_posts) / 12, 2) AS average_posts_per_month
FROM monthly_counts
LIMIT 1
