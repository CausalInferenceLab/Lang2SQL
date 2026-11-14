WITH Bowler_Venue_Wins AS (
    SELECT
        bs.BowlerID,
        b.BowlerFirstName,
        b.BowlerLastName,
        mg.MatchNo,
        mg.GameNo,
        bs.HandiCapScore,
        t.TourneyDate,
        t.TourneyLocation
    FROM
        Bowler_Scores bs
        JOIN Match_Games mg ON bs.MatchNo = mg.MatchNo AND bs.GameNo = mg.GameNo
        JOIN Tournaments t ON mg.TourneyID = t.TourneyID
        JOIN Bowlers b ON bs.BowlerID = b.BowlerID
    WHERE
        bs.HandiCapScore <= 190
        AND bs.GameResult = 'Win'
        AND t.TourneyLocation IN ('Thunderbird Lanes', 'Totem Lanes', 'Bolero Lanes')
),
Bowlers_All_Three_Venues AS (
    SELECT
        BowlerID
    FROM
        Bowler_Venue_Wins
    GROUP BY
        BowlerID
    HAVING
        COUNT(DISTINCT TourneyLocation) = 3
)
SELECT
    bw.BowlerID,
    bw.BowlerFirstName,
    bw.BowlerLastName,
    bw.MatchNo,
    bw.GameNo,
    bw.HandiCapScore,
    bw.TourneyDate,
    bw.TourneyLocation
FROM
    Bowler_Venue_Wins bw
    INNER JOIN Bowlers_All_Three_Venues b3v ON bw.BowlerID = b3v.BowlerID
ORDER BY
    bw.BowlerID,
    bw.TourneyLocation,
    bw.MatchNo,
    bw.GameNo;
