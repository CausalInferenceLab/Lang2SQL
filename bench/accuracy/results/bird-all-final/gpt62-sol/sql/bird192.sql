WITH last_match AS (
    SELECT t.team_api_id, MAX(m.date) AS match_date
    FROM Team AS t
    JOIN "Match" AS m
      ON t.team_api_id IN (m.home_team_api_id, m.away_team_api_id)
    WHERE t.team_long_name = 'KSV Cercle Brugge'
      AND m.season = '2012/2013'
    GROUP BY t.team_api_id
)
SELECT a.buildUpPlaySpeedClass
FROM last_match AS lm
JOIN Team_Attributes AS a
  ON a.team_api_id = lm.team_api_id
 AND a.date <= lm.match_date
ORDER BY a.date DESC, a.id DESC
LIMIT 1;
