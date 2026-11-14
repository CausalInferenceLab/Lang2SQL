WITH HomeWins AS (
  SELECT Div AS League, HomeTeam AS Team, COUNT(*) AS Wins
  FROM Match
  WHERE FTHG > FTAG
  GROUP BY Div, HomeTeam
),
AwayWins AS (
  SELECT Div AS League, AwayTeam AS Team, COUNT(*) AS Wins
  FROM Match
  WHERE FTAG > FTHG
  GROUP BY Div, AwayTeam
),
TotalWins AS (
  SELECT League, Team, SUM(Wins) AS Wins
  FROM (
    SELECT * FROM HomeWins
    UNION ALL
    SELECT * FROM AwayWins
  )
  GROUP BY League, Team
),
LeagueTeams AS (
  SELECT Div AS League, HomeTeam AS Team FROM Match
  UNION
  SELECT Div AS League, AwayTeam AS Team FROM Match
),
WinsWithZero AS (
  SELECT LT.League, LT.Team, COALESCE(TW.Wins, 0) AS Wins
  FROM LeagueTeams LT
  LEFT JOIN TotalWins TW ON LT.League = TW.League AND LT.Team = TW.Team
),
MinWins AS (
  SELECT League, MIN(Wins) AS MinWin
  FROM WinsWithZero
  GROUP BY League
),
TiedMinWins AS (
  SELECT WW.League, WW.Team, WW.Wins
  FROM WinsWithZero WW
  JOIN MinWins MW ON WW.League = MW.League AND WW.Wins = MW.MinWin
),
SingleMinWinPerLeague AS (
  SELECT League, Team, Wins
  FROM (
    SELECT League, Team, Wins,
           ROW_NUMBER() OVER (PARTITION BY League ORDER BY Team) AS rn
    FROM TiedMinWins
  )
  WHERE rn = 1
)
SELECT League, Team, Wins
FROM SingleMinWinPerLeague
ORDER BY League;
