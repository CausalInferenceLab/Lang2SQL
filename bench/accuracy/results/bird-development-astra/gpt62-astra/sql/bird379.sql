SELECT ROUND(100.0 * AVG(CASE WHEN a.alignment = 'Good' THEN 1 ELSE 0 END), 2) AS good_percentage
FROM superhero AS h
JOIN publisher AS p ON p.id = h.publisher_id
LEFT JOIN alignment AS a ON a.id = h.alignment_id
WHERE p.publisher_name = 'Marvel Comics'
  AND EXISTS (
    SELECT 1
    FROM hero_power AS hp
    JOIN superpower AS sp ON sp.id = hp.power_id
    WHERE hp.hero_id = h.id
  );
