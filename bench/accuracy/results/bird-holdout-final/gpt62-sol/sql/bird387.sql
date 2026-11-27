SELECT ROUND(
    100.0 * COUNT(DISTINCT CASE WHEN p.SEX = 'F' THEN p.ID END)
    / NULLIF(COUNT(DISTINCT p.ID), 0),
    2
) AS female_percentage
FROM Patient AS p
WHERE DATE(p.Birthday) <= DATE('2026-10-09', '-56 years')
  AND EXISTS (
      SELECT 1
      FROM Laboratory AS l
      WHERE l.ID = p.ID
        AND l.PT >= 14
  );
