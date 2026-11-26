SELECT COUNT(DISTINCT c.uuid) AS printing_count
FROM cards AS c
WHERE (c.power IS NULL OR instr(c.power, '*') > 0)
  AND c.uuid IN (
      SELECT r.uuid
      FROM rulings AS r
      WHERE r.text LIKE '%triggered ability%'
  )
LIMIT 1
