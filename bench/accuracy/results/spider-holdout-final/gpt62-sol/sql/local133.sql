WITH style_scores AS (
    SELECT StyleID,
           SUM(CASE PreferenceSeq
                   WHEN 1 THEN 3
                   WHEN 2 THEN 2
                   WHEN 3 THEN 1
                   ELSE 0
               END) AS total_weighted_score
    FROM Musical_Preferences
    GROUP BY StyleID
)
SELECT ms.StyleName,
       ss.total_weighted_score,
       ABS(ss.total_weighted_score - AVG(ss.total_weighted_score) OVER ()) AS absolute_difference
FROM style_scores AS ss
JOIN Musical_Styles AS ms ON ms.StyleID = ss.StyleID;
