WITH editor_counts AS (
    SELECT u.DisplayName AS display_name,
           COUNT(*) AS edit_comment_count
    FROM posts AS p
    JOIN postHistory AS h ON h.PostId = p.Id
    JOIN users AS u ON u.Id = h.UserId
    WHERE p.Title = 'Why square the difference instead of taking the absolute value in standard deviation?'
      AND NULLIF(TRIM(h.Comment), '') IS NOT NULL
    GROUP BY u.Id, u.DisplayName
)
SELECT display_name, edit_comment_count
FROM editor_counts
WHERE edit_comment_count = (SELECT MAX(edit_comment_count) FROM editor_counts)
ORDER BY display_name;
