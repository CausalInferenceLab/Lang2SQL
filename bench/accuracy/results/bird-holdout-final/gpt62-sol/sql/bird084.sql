SELECT COUNT(DISTINCT artist) AS artist_count
FROM cards
WHERE borderColor = 'black'
  AND instr(',' || availability || ',', ',arena,') > 0
  AND instr(',' || availability || ',', ',mtgo,') = 0
  AND power <> ''
  AND power NOT GLOB '*[^0-9]*'
  AND CAST(power AS INTEGER) >= 5;
