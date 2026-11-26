WITH frame_counts AS (
    SELECT CAST(frameVersion AS INTEGER) AS frame_year,
           COUNT(*) AS card_count
    FROM cards
    WHERE artist = 'John Avon'
      AND NULLIF(TRIM(cardKingdomId), '') IS NOT NULL
      AND NULLIF(TRIM(cardKingdomFoilId), '') IS NOT NULL
      AND frameVersion GLOB '[0-9][0-9][0-9][0-9]'
    GROUP BY CAST(frameVersion AS INTEGER)
)
SELECT frame_year, card_count
FROM frame_counts
WHERE frame_year = (SELECT MAX(frame_year) FROM frame_counts);
