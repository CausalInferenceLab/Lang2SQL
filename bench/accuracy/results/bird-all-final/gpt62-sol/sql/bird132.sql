WITH posts_2012 AS (
    SELECT LastEditorUserId, Score
    FROM posts
    WHERE CreaionDate >= '2012-01-01'
      AND CreaionDate < '2013-01-01'
),
german_editors AS (
    SELECT u.Id, u.Reputation
    FROM users AS u
    JOIN (SELECT DISTINCT LastEditorUserId FROM posts_2012) AS e
      ON e.LastEditorUserId = u.Id
    WHERE LOWER(TRIM(u.Location)) = 'germany'
       OR LOWER(TRIM(u.Location)) LIKE '%, germany'
       OR LOWER(TRIM(u.Location)) = 'munich'
)
SELECT
    SUM(p.Score) FILTER (WHERE g.Id IS NOT NULL) AS germany_edited_total_score,
    SUM(p.Score) FILTER (WHERE g.Id IS NULL) AS other_posts_total_score,
    (SELECT COUNT(*) FROM german_editors) AS german_editor_count,
    (SELECT SUM(Reputation) FROM german_editors) AS german_editors_total_reputation
FROM posts_2012 AS p
LEFT JOIN german_editors AS g ON g.Id = p.LastEditorUserId;
