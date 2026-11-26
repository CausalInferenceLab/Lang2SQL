SELECT ROUND(100.0 * COUNT(CASE WHEN a.alignment = 'Good' THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS percentage
FROM superhero AS s
JOIN publisher AS p ON p.id = s.publisher_id
LEFT JOIN alignment AS a ON a.id = s.alignment_id
WHERE p.publisher_name = 'Marvel Comics'
  AND EXISTS (
    SELECT 1
    FROM hero_power AS hp
    JOIN superpower AS sp ON sp.id = hp.power_id
    WHERE hp.hero_id = s.id
  );
