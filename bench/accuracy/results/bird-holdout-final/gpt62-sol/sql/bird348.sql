WITH intelligence_scores AS (
    SELECT ha.hero_id, ha.attribute_value
    FROM hero_attribute AS ha
    JOIN attribute AS a ON a.id = ha.attribute_id
    WHERE a.attribute_name = 'Intelligence'
), abomination_scores AS (
    SELECT i.attribute_value
    FROM intelligence_scores AS i
    JOIN superhero AS s ON s.id = i.hero_id
    WHERE s.superhero_name = 'Abomination'
)
SELECT ROUND(AVG(s.weight_kg), 2) AS average_weight_kg
FROM superhero AS s
WHERE s.weight_kg > 0
  AND s.id IN (
      SELECT i.hero_id
      FROM intelligence_scores AS i
      WHERE i.attribute_value IN (SELECT attribute_value FROM abomination_scores)
  );
