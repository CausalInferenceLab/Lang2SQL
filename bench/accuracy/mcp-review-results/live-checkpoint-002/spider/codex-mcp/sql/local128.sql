WITH qualifying_games AS (
    SELECT s.BowlerID, s.MatchID, s.GameNumber, s.HandiCapScore,
           t.TourneyDate, t.TourneyLocation
    FROM main.Bowler_Scores AS s
    JOIN main.Tourney_Matches AS m ON m.MatchID = s.MatchID
    JOIN main.Tournaments AS t ON t.TourneyID = m.TourneyID
    WHERE s.WonGame = 1
      AND s.HandiCapScore <= 190
      AND t.TourneyLocation IN ('Thunderbird Lanes', 'Totem Lanes', 'Bolero Lanes')
)
SELECT b.BowlerID, b.BowlerFirstName, b.BowlerLastName,
       g.MatchID, g.GameNumber, g.HandiCapScore, g.TourneyDate, g.TourneyLocation
FROM qualifying_games AS g
JOIN main.Bowlers AS b ON b.BowlerID = g.BowlerID
WHERE g.BowlerID IN (
    SELECT BowlerID
    FROM qualifying_games
    GROUP BY BowlerID
    HAVING COUNT(DISTINCT TourneyLocation) = 3
)
ORDER BY b.BowlerID, g.MatchID, g.GameNumber
LIMIT 1000
