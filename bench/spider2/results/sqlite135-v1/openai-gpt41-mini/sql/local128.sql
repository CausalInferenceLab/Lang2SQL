WITH WinningGames AS (
  SELECT 
    b.id AS bowler_id,
    b.first_name,
    b.last_name,
    mg.match_number,
    mg.game_number,
    mg.handicap_score,
    t.tournament_date,
    t.location
  FROM Bowler_Scores bs
  JOIN Bowlers b ON bs.bowler_id = b.id
  JOIN Match_Games mg ON bs.match_game_id = mg.id
  JOIN Tournaments t ON mg.tournament_id = t.id
  WHERE bs.won = 1
    AND mg.handicap_score <= 190
    AND t.location IN ('Thunderbird Lanes', 'Totem Lanes', 'Bolero Lanes')
),
QualifiedBowlers AS (
  SELECT bowler_id
  FROM WinningGames
  GROUP BY bowler_id
  HAVING COUNT(DISTINCT location) = 3
)
SELECT 
  wg.bowler_id,
  wg.first_name,
  wg.last_name,
  wg.match_number,
  wg.game_number,
  wg.handicap_score,
  wg.tournament_date,
  wg.location
FROM WinningGames wg
JOIN QualifiedBowlers qb ON wg.bowler_id = qb.bowler_id
ORDER BY wg.bowler_id, wg.tournament_date, wg.match_number, wg.game_number;
