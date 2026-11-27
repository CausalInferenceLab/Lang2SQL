SELECT ROUND(100.0 * COUNT(CASE WHEN p.SEX = 'F' THEN 1 END) / NULLIF(COUNT(*), 0), 1) AS women_percentage
FROM Patient AS p
WHERE p.SEX IN ('F', 'M')
  AND strftime('%Y', p.Birthday) BETWEEN '1936' AND '1956'
  AND NOT EXISTS (
    SELECT 1
    FROM Laboratory AS l
    WHERE l.ID = p.ID AND l.CPK >= 250
  );
