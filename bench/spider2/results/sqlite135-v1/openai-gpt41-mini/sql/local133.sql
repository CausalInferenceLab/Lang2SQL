WITH StyleScores AS (
  SELECT
    MusicalStyleID,
    SUM(CASE PreferenceSeq
            WHEN 1 THEN 3
            WHEN 2 THEN 2
            WHEN 3 THEN 1
            ELSE 0
        END) AS TotalScore
  FROM Musical_Preferences
  WHERE PreferenceSeq IN (1, 2, 3)
  GROUP BY MusicalStyleID
),
AverageScore AS (
  SELECT AVG(TotalScore) AS AvgScore FROM StyleScores
)
SELECT
  ss.MusicalStyleID,
  ss.TotalScore,
  ABS(ss.TotalScore - a.AvgScore) AS AbsDifferenceFromAvg
FROM StyleScores ss, AverageScore a;
