WITH badge_totals AS (
    SELECT u.Id, u.DisplayName, COUNT(b.Id) AS badge_total
    FROM users AS u
    LEFT JOIN badges AS b ON b.UserId = u.Id
    WHERE LOWER(u.Location) LIKE '%germany%'
    GROUP BY u.Id, u.DisplayName
), ranked_users AS (
    SELECT DisplayName, badge_total,
           DENSE_RANK() OVER (ORDER BY badge_total DESC) AS badge_rank
    FROM badge_totals
)
SELECT DisplayName, badge_total
FROM ranked_users
WHERE badge_rank = 2
ORDER BY DisplayName;
