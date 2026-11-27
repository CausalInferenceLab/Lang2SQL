WITH strength_scores AS (
    SELECT ha.hero_id, ha.attribute_value
    FROM hero_attribute AS ha
    JOIN attribute AS a ON a.id = ha.attribute_id
    WHERE a.attribute_name = 'Strength'
)
SELECT DISTINCT s.full_name
FROM superhero AS s
JOIN strength_scores AS ss ON ss.hero_id = s.id
WHERE ss.attribute_value = (SELECT MAX(attribute_value) FROM strength_scores)
  AND TRIM(s.full_name) NOT IN ('', '-')
ORDER BY s.full_name;
