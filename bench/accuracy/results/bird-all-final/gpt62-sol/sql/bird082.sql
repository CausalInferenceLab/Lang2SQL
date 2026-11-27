SELECT COUNT(DISTINCT c.name) AS card_count
FROM cards AS c
WHERE c.hasContentWarning = 1
  AND instr(',' || c.availability || ',', ',paper,') > 0
  AND NOT EXISTS (
    SELECT 1
    FROM cards AS m
    WHERE m.name = c.name
      AND instr(',' || m.availability || ',', ',mtgo,') > 0
  );
