SELECT DISTINCT ta.buildUpPlayDribblingClass
FROM Team_Attributes AS ta
JOIN Team AS t ON t.team_api_id = ta.team_api_id
WHERE t.team_short_name = 'POR'
  AND date(ta.date) = '2015-09-10'
  AND EXISTS (
    SELECT 1
    FROM Match AS m
    JOIN Country AS c ON c.id = m.country_id
    WHERE c.name = 'England'
      AND t.team_api_id IN (m.home_team_api_id, m.away_team_api_id)
  );
