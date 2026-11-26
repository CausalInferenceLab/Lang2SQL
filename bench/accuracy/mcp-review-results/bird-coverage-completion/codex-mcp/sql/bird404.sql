WITH tested_women AS (
    SELECT p.ID,
           MAX(CASE WHEN l.RNP = 'negative' THEN 0
                    ELSE CAST(l.RNP AS INTEGER)
               END) AS highest_titer
    FROM main.Patient AS p
    JOIN main.Laboratory AS l ON l.ID = p.ID
    WHERE p.SEX = 'F' AND l.RNP IS NOT NULL
    GROUP BY p.ID
)
SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE highest_titer > 64)
             / NULLIF(COUNT(*), 0), 2) AS elevated_percentage
FROM tested_women
LIMIT 1
