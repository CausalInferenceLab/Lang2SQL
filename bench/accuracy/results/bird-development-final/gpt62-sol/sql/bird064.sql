SELECT COUNT(*) AS card_count
FROM cards
WHERE borderColor = 'black'
  AND instr(',' || availability || ',', ',mtgo,') > 0
  AND CAST(power AS INTEGER) >= 10;
