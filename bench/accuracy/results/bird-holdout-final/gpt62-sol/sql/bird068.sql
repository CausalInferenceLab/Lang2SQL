WITH artist_counts AS (
    SELECT artist, COUNT(DISTINCT name) AS creature_card_count
    FROM cards
    WHERE ',' || types || ',' LIKE '%,Creature,%'
      AND artist IS NOT NULL
    GROUP BY artist
)
SELECT artist, creature_card_count
FROM artist_counts
WHERE creature_card_count = (SELECT MAX(creature_card_count) FROM artist_counts)
ORDER BY artist;
