SELECT MAX(CAST(frameVersion AS INTEGER)) - MIN(CAST(frameVersion AS INTEGER)) AS span_in_years
FROM cards
WHERE name = 'Tarmogoyf'
  AND frameVersion GLOB '[0-9][0-9][0-9][0-9]';
