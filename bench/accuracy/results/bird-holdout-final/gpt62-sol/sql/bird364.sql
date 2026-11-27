SELECT ROUND(AVG(ha.attribute_value), 1) AS average_intelligence
FROM superhero AS h
JOIN hero_attribute AS ha ON ha.hero_id = h.id
JOIN attribute AS a ON a.id = ha.attribute_id
WHERE LOWER(a.attribute_name) = 'intelligence'
  AND ha.attribute_value IS NOT NULL
  AND EXISTS (
    SELECT 1
    FROM hero_power AS hp
    JOIN superpower AS sp ON sp.id = hp.power_id
    WHERE hp.hero_id = h.id
      AND LOWER(sp.power_name) IN ('super speed', 'death touch')
  );
