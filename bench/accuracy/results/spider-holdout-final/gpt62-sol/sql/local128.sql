WITH qualifying_games AS (
    SELECT s.BowlerID,
           s.MatchID,
           s.GameNumber,
           s.HandiCapScore,
           t.TourneyDate,
           t.TourneyLocation
    FROM Bowler_Scores AS s
    JOIN Tourney_Matches AS m ON m.MatchID = s.MatchID
    JOIN Tournaments AS t ON t.TourneyID = m.TourneyID
    WHERE s.WonGame = 1
      AND s.HandiCapScore <= 190
      AND t.TourneyLocation IN ('Thunderbird Lanes', 'Totem Lanes', 'Bolero Lanes')
), qualifying_bowlers AS (
    SELECT BowlerID
    FROM qualifying_games
    GROUP BY BowlerID
    HAVING COUNT(DISTINCT TourneyLocation) = 3
)
SELECT b.BowlerID,
       b.BowlerFirstName,
       b.BowlerLastName,
       g.MatchID,
       g.GameNumber,
       g.HandiCapScore,
       g.TourneyDate,
       g.TourneyLocation
FROM qualifying_games AS g
JOIN qualifying_bowlers AS q ON q.BowlerID = g.BowlerID
JOIN Bowlers AS b ON b.BowlerID = g.BowlerID
ORDER BY b.BowlerID, g.MatchID, g.GameNumber;
