WITH artist_counts AS (
    SELECT artist, COUNT(*) AS card_count
    FROM cards
    WHERE borderColor = 'black'
      AND instr(',' || availability || ',', ',arena,') > 0
      AND trim(power) NOT GLOB '*[^0-9]*'
      AND CAST(trim(power) AS INTEGER) >= 10
    GROUP BY artist
)
SELECT artist, card_count
FROM artist_counts
WHERE card_count = (SELECT MAX(card_count) FROM artist_counts)
ORDER BY artist;
