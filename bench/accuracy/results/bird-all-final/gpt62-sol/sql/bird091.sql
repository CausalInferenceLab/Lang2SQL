SELECT COUNT(*) AS printing_count
FROM cards AS c
WHERE c.artist = 'Erica Yang'
  AND INSTR(',' || c.availability || ',', ',paper,') > 0
  AND EXISTS (
    SELECT 1
    FROM legalities AS l
    WHERE l.uuid = c.uuid
      AND l.format = 'pauper'
      AND l.status = 'Legal'
  );
