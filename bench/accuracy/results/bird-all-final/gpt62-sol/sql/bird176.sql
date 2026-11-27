WITH eligible_clubs AS (
    SELECT t.team_api_id, t.team_short_name
    FROM Team AS t
    WHERE EXISTS (
        SELECT 1
        FROM Team_Attributes AS a
        WHERE a.team_api_id = t.team_api_id
          AND a.chanceCreationPassingClass = 'Risky'
    )
), ranked_attributes AS (
    SELECT team_api_id, chanceCreationPassing,
           ROW_NUMBER() OVER (
               PARTITION BY team_api_id
               ORDER BY date DESC, id DESC
           ) AS rn
    FROM Team_Attributes
    WHERE date < '2014-09-20'
)
SELECT c.team_short_name AS short_name,
       COUNT(*) AS club_count,
       ROUND(AVG(a.chanceCreationPassing), 1) AS avg_passing_risk
FROM eligible_clubs AS c
LEFT JOIN ranked_attributes AS a
    ON a.team_api_id = c.team_api_id AND a.rn = 1
GROUP BY c.team_short_name
HAVING COUNT(*) > 1
ORDER BY avg_passing_risk DESC, short_name ASC;
