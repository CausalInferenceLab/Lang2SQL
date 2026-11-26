SELECT COUNT(*) AS printing_count
FROM main.cards
WHERE instr(',' || keywords || ',', ',Flying,') > 0
  AND instr(',' || keywords || ',', ',Cycling,') > 0
LIMIT 1
