SELECT
    t.team_long_name AS club_name,
    t.team_short_name AS short_name,
    COUNT(*) AS home_wins
FROM "Match" AS m
JOIN Team AS t ON t.team_api_id = m.home_team_api_id
WHERE m.home_team_goal - m.away_team_goal >= 3
  AND (
      SELECT ta.buildUpPlaySpeedClass
      FROM Team_Attributes AS ta
      WHERE ta.team_api_id = m.home_team_api_id
        AND ta.date <= m.date
      ORDER BY ta.date DESC, ta.id DESC
      LIMIT 1
  ) = 'Fast'
GROUP BY t.team_api_id, t.team_long_name, t.team_short_name
ORDER BY home_wins DESC, club_name, short_name, t.team_api_id;
