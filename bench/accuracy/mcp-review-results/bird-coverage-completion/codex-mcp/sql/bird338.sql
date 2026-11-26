WITH strength AS (
    SELECT ha.hero_id, ha.attribute_value
    FROM main.hero_attribute AS ha
    JOIN main.attribute AS a ON a.id = ha.attribute_id
    WHERE a.attribute_name = 'Strength'
)
SELECT s.full_name
FROM main.superhero AS s
WHERE s.full_name NOT IN ('', '-')
  AND s.id IN (
      SELECT hero_id
      FROM strength
      WHERE attribute_value = (SELECT MAX(attribute_value) FROM strength)
  )
ORDER BY s.full_name
LIMIT -1
