WITH elevated_rnp AS (
    SELECT DISTINCT ID
    FROM Laboratory
    WHERE CAST(RNP AS REAL) >= 16
), normal_gpt AS (
    SELECT l.ID, MAX(l.GPT) AS highest_normal_gpt
    FROM Laboratory AS l
    JOIN elevated_rnp AS e ON e.ID = l.ID
    WHERE l.GPT < 60
    GROUP BY l.ID
)
SELECT p.Birthday
FROM normal_gpt AS g
JOIN Patient AS p ON p.ID = g.ID
ORDER BY g.highest_normal_gpt DESC, g.ID ASC
LIMIT 3;
