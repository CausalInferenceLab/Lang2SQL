SELECT p.player_name,
       COUNT(DISTINCT m.match_api_id) AS matches_played
FROM Player AS p
LEFT JOIN "Match" AS m
  ON m.season = '2015/2016'
 AND p.player_api_id IN (
       m.home_player_1, m.home_player_2, m.home_player_3, m.home_player_4,
       m.home_player_5, m.home_player_6, m.home_player_7, m.home_player_8,
       m.home_player_9, m.home_player_10, m.home_player_11,
       m.away_player_1, m.away_player_2, m.away_player_3, m.away_player_4,
       m.away_player_5, m.away_player_6, m.away_player_7, m.away_player_8,
       m.away_player_9, m.away_player_10, m.away_player_11
     )
WHERE p.player_name LIKE 'Adam%'
  AND p.weight > 170
GROUP BY p.player_api_id, p.player_name;
