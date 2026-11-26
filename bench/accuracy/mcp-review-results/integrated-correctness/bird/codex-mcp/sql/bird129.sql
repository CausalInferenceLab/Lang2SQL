WITH organizer_awards AS (
    SELECT o.UserId, MIN(o.Date) AS organizer_date
    FROM badges AS o
    JOIN users AS u ON u.Id = o.UserId
    WHERE o.Name = 'Organizer'
      AND (
          u.Location IN ('Germany', 'Munich', 'Minga')
          OR u.Location LIKE '%, Germany'
      )
    GROUP BY o.UserId
),
qualifying_users AS (
    SELECT o.UserId
    FROM organizer_awards AS o
    JOIN badges AS b
      ON b.UserId = o.UserId
     AND b.Date < o.organizer_date
    GROUP BY o.UserId
    HAVING COUNT(*) = 5
)
SELECT COUNT(*) AS user_count
FROM qualifying_users
LIMIT 1
