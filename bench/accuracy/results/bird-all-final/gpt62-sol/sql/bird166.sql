SELECT l.name
FROM League AS l
JOIN "Match" AS m ON m.league_id = l.id
WHERE m.season = '2009/2010'
GROUP BY l.id, l.name
HAVING AVG(m.home_team_goal) > AVG(m.away_team_goal)
ORDER BY l.name;
