SELECT DISTINCT s.name AS set_name, s.releaseDate AS release_date
FROM cards AS c
JOIN sets AS s ON s.code = c.setCode
WHERE c.name = 'Tarmogoyf'
  AND c.isReprint = 1
  AND c.frameVersion GLOB '[0-9][0-9][0-9][0-9]'
  AND CAST(strftime('%Y', s.releaseDate) AS INTEGER)
      - CAST(c.frameVersion AS INTEGER) >= 10
ORDER BY release_date, set_name;
