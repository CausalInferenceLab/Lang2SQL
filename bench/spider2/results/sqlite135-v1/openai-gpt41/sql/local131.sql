SELECT
  ms.StyleName,
  SUM(CASE WHEN mp.PreferenceRank = 1 THEN 1 ELSE 0 END) AS first_pref_count,
  SUM(CASE WHEN mp.PreferenceRank = 2 THEN 1 ELSE 0 END) AS second_pref_count,
  SUM(CASE WHEN mp.PreferenceRank = 3 THEN 1 ELSE 0 END) AS third_pref_count
FROM Musical_Styles ms
LEFT JOIN Musical_Preferences mp ON ms.StyleID = mp.StyleID
GROUP BY ms.StyleName
ORDER BY ms.StyleName;
