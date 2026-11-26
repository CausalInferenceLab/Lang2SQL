SELECT COUNT(DISTINCT u.DisplayName) AS distinct_display_names
FROM users AS u
JOIN badges AS b ON b.UserId = u.Id
WHERE b.Name = 'Supporter'
  AND u.Age BETWEEN 19 AND 65
  AND u.DisplayName <> 'NaN'
LIMIT 1000
