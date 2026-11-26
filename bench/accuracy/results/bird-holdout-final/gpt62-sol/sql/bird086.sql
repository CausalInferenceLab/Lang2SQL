SELECT frameVersion AS frame_year, COUNT(*) AS card_count
FROM cards
WHERE cardKingdomId IS NOT NULL
  AND cardKingdomFoilId IS NOT NULL
  AND leadershipSkills LIKE '%''commander'': True%'
  AND frameVersion GLOB '[0-9][0-9][0-9][0-9]'
GROUP BY frameVersion
ORDER BY frame_year DESC
LIMIT 1;
