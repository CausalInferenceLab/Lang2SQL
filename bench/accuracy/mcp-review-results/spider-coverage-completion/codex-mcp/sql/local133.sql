WITH style_scores AS (
    SELECT
        s.StyleName,
        SUM(CASE p.PreferenceSeq
            WHEN 1 THEN 3
            WHEN 2 THEN 2
            WHEN 3 THEN 1
            ELSE 0
        END) AS total_weighted_score
    FROM main.Musical_Preferences AS p
    JOIN main.Musical_Styles AS s ON s.StyleID = p.StyleID
    GROUP BY s.StyleID, s.StyleName
)
SELECT
    StyleName,
    total_weighted_score,
    ABS(total_weighted_score - AVG(total_weighted_score) OVER ()) AS absolute_difference
FROM style_scores
LIMIT 1000
