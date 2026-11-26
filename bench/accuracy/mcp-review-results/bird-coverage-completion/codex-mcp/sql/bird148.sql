WITH editor_counts AS (
    SELECT h.UserId, COUNT(DISTINCT h.RevisionGUID) AS edit_comment_count
    FROM main.posts AS p
    JOIN main.postHistory AS h ON h.PostId = p.Id
    WHERE p.Title = 'Why square the difference instead of taking the absolute value in standard deviation?'
      AND TRIM(h.Comment) <> ''
    GROUP BY h.UserId
)
SELECT u.DisplayName, e.edit_comment_count
FROM editor_counts AS e
JOIN main.users AS u ON u.Id = e.UserId
WHERE e.edit_comment_count = (SELECT MAX(edit_comment_count) FROM editor_counts)
ORDER BY u.DisplayName
LIMIT -1
