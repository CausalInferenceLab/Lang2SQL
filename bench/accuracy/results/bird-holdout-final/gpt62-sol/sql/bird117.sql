SELECT COUNT(DISTINCT c.scryfallOracleId) AS card_count
FROM cards AS c
WHERE c.colors = 'B'
  AND instr(',' || c.types || ',', ',Artifact,') > 0
  AND EXISTS (
    SELECT 1
    FROM foreign_data AS f
    WHERE f.uuid = c.uuid
      AND f.language <> 'English'
      AND TRIM(f.flavorText) <> ''
  );
