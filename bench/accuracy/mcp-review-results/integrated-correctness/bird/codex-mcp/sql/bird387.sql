SELECT ROUND(
    100.0 * COUNT(CASE WHEN p.SEX = 'F' THEN 1 END) / NULLIF(COUNT(*), 0),
    2
) AS female_percentage
FROM main.Patient AS p
WHERE DATE(p.Birthday) <= DATE('2026-10-09', '-56 years')
  AND EXISTS (
      SELECT 1
      FROM main.Laboratory AS l
      WHERE l.ID = p.ID
        AND l.PT >= 14
  )
LIMIT 1
