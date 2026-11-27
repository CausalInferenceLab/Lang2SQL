WITH linked_posts AS (
    SELECT DISTINCT strftime('%Y-%m', l.CreationDate) AS month, p.Id AS post_id
    FROM postLinks AS l
    JOIN posts AS p ON p.Id = l.RelatedPostId
    WHERE l.CreationDate >= '2010-01-01'
      AND l.CreationDate < '2011-01-01'
      AND p.AnswerCount <= 2
)
SELECT ROUND(COUNT(*) / 12.0, 2) AS average_monthly_posts
FROM linked_posts;
