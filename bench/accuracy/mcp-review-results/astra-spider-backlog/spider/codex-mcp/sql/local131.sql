SELECT
    s.StyleName,
    COUNT(*) FILTER (WHERE p.PreferenceSeq = 1) AS FirstPreferenceCount,
    COUNT(*) FILTER (WHERE p.PreferenceSeq = 2) AS SecondPreferenceCount,
    COUNT(*) FILTER (WHERE p.PreferenceSeq = 3) AS ThirdPreferenceCount
FROM Musical_Styles AS s
LEFT JOIN Musical_Preferences AS p ON p.StyleID = s.StyleID
GROUP BY s.StyleID, s.StyleName
ORDER BY s.StyleName
LIMIT 1000
