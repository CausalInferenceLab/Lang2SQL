SELECT DISTINCT UserId
FROM (
    SELECT ph.UserId, ph.PostId
    FROM postHistory AS ph
    JOIN posts AS p ON p.Id = ph.PostId
    WHERE p.ViewCount >= 1000
      AND ph.UserId IS NOT NULL
    GROUP BY ph.UserId, ph.PostId
    HAVING COUNT(*) = 1
) AS qualifying_user_posts;
