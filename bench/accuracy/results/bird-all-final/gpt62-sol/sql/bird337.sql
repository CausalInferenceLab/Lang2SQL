SELECT ROUND(100.0 * AVG(CASE WHEN EXISTS (
    SELECT 1
    FROM publisher AS p
    WHERE p.id = s.publisher_id
      AND p.publisher_name = 'Marvel Comics'
) THEN 1 ELSE 0 END), 2) AS marvel_percentage
FROM superhero AS s
WHERE s.height_cm BETWEEN 150 AND 180;
