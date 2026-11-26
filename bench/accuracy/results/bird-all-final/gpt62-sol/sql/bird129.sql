WITH organizer_awards AS (
    SELECT b.UserId, MIN(b.Date) AS organizer_date
    FROM badges AS b
    JOIN users AS u ON u.Id = b.UserId
    WHERE b.Name = 'Organizer'
      AND u.Location LIKE '%Germany%'
    GROUP BY b.UserId
)
SELECT COUNT(*) AS user_count
FROM organizer_awards AS o
WHERE (
    SELECT COUNT(*)
    FROM badges AS b
    WHERE b.UserId = o.UserId
      AND b.Date < o.organizer_date
) = 5;
