SELECT COUNT(DISTINCT NULLIF(u.DisplayName, 'NaN')) AS distinct_display_names
FROM users AS u
WHERE u.Age BETWEEN 19 AND 65
  AND EXISTS (
    SELECT 1
    FROM badges AS b
    WHERE b.UserId = u.Id AND b.Name = 'Supporter'
  );
