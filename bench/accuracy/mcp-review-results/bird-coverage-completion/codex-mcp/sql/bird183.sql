WITH country_counts AS (
    SELECT c.name AS country,
           COUNT(DISTINCT p.player_api_id) AS qualifying_players
    FROM "Match" AS m
    JOIN League AS l ON l.id = m.league_id
    JOIN Country AS c ON c.id = l.country_id
    LEFT JOIN Player AS p
      ON p.weight >= 210
     AND p.player_api_id IN (
         m.home_player_1, m.home_player_2, m.home_player_3, m.home_player_4,
         m.home_player_5, m.home_player_6, m.home_player_7, m.home_player_8,
         m.home_player_9, m.home_player_10, m.home_player_11,
         m.away_player_1, m.away_player_2, m.away_player_3, m.away_player_4,
         m.away_player_5, m.away_player_6, m.away_player_7, m.away_player_8,
         m.away_player_9, m.away_player_10, m.away_player_11
     )
    GROUP BY c.id, c.name
)
SELECT country, qualifying_players
FROM country_counts
WHERE qualifying_players = (SELECT MAX(qualifying_players) FROM country_counts)
ORDER BY country
LIMIT 1000
