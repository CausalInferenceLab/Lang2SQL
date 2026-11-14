WITH PreferenceCounts AS (
  SELECT 
    ms.style_name,
    SUM(CASE WHEN mp.preference_rank = 1 THEN 1 ELSE 0 END) AS first_pref_count,
    SUM(CASE WHEN mp.preference_rank = 2 THEN 1 ELSE 0 END) AS second_pref_count,
    SUM(CASE WHEN mp.preference_rank = 3 THEN 1 ELSE 0 END) AS third_pref_count
  FROM Musical_Styles ms
  LEFT JOIN Musical_Preferences mp ON ms.style_id = mp.style_id
  GROUP BY ms.style_name
)
SELECT 
  style_name,
  first_pref_count,
  second_pref_count,
  third_pref_count
FROM PreferenceCounts
ORDER BY style_name;
