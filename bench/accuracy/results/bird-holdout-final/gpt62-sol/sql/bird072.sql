SELECT COUNT(*) AS card_count
FROM cards
WHERE instr(',' || keywords || ',', ',Flying,') > 0
  AND instr(',' || keywords || ',', ',Cycling,') > 0;
