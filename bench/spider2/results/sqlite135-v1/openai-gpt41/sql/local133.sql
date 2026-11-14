WITH style_scores AS (
    SELECT
        mp.Musical_Style_ID,
        ms.StyleName,
        SUM(
            CASE mp.PreferenceSeq
                WHEN 1 THEN 3
                WHEN 2 THEN 2
                WHEN 3 THEN 1
                ELSE 0
            END
        ) AS total_weighted_score
    FROM Musical_Preferences mp
    JOIN Musical_Styles ms ON mp.Musical_Style_ID = ms.Musical_Style_ID
    GROUP BY mp.Musical_Style_ID, ms.StyleName
),
avg_score AS (
    SELECT AVG(total_weighted_score) AS avg_total_weighted_score
    FROM style_scores
)
SELECT
    s.Musical_Style_ID,
    s.StyleName,
    s.total_weighted_score,
    ROUND(ABS(s.total_weighted_score - a.avg_total_weighted_score), 2) AS abs_diff_from_avg
FROM style_scores s
CROSS JOIN avg_score a
ORDER BY s.total_weighted_score DESC, s.Musical_Style_ID;
