WITH year_counts AS (
    SELECT frameVersion AS frame_year, COUNT(*) AS card_count
    FROM main.cards
    WHERE artist = 'John Avon'
      AND cardKingdomId IS NOT NULL
      AND cardKingdomFoilId IS NOT NULL
      AND frameVersion GLOB '[0-9][0-9][0-9][0-9]'
    GROUP BY frameVersion
)
SELECT frame_year, card_count
FROM year_counts
WHERE frame_year = (SELECT MAX(frame_year) FROM year_counts)
LIMIT 1000
