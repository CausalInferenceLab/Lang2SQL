WITH speeds AS (
    SELECT ha.hero_id, ha.attribute_value
    FROM main.hero_attribute AS ha
    JOIN main.attribute AS a ON a.id = ha.attribute_id
    WHERE a.attribute_name = 'Speed'
)
SELECT s.superhero_name, s.full_name
FROM main.superhero AS s
WHERE s.id IN (
    SELECT hero_id
    FROM speeds
    WHERE attribute_value = (SELECT MAX(attribute_value) FROM speeds)
)
AND TRIM(s.full_name) NOT IN ('', '-')
ORDER BY s.superhero_name, s.full_name
LIMIT -1
