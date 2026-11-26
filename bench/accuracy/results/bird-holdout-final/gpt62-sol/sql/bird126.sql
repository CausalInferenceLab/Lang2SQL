SELECT COUNT(*) AS zero_score_comment_count
FROM (
    SELECT PostId
    FROM comments
    GROUP BY PostId
    HAVING COUNT(*) = 1 AND MAX(Score) = 0
) AS single_comment
WHERE EXISTS (
    SELECT 1
    FROM posts AS p
    WHERE p.Id = single_comment.PostId
);
