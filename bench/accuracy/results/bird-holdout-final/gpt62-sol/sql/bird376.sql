WITH speed AS (
  SELECT ha.hero_id, ha.attribute_value
  FROM hero_attribute AS ha
  JOIN attribute AS a ON a.id = ha.attribute_id
  WHERE a.attribute_name = 'Speed'
)
SELECT s.superhero_name, s.full_name
FROM superhero AS s
WHERE TRIM(s.full_name) NOT IN ('', '-')
  AND EXISTS (
    SELECT 1
    FROM speed AS sp
    WHERE sp.hero_id = s.id
      AND sp.attribute_value = (SELECT MAX(attribute_value) FROM speed)
  )
ORDER BY s.superhero_name, s.full_name;
