SELECT COUNT(DISTINCT c.scryfallOracleId) AS card_count
FROM cards AS c
JOIN foreign_data AS f ON f.uuid = c.uuid
WHERE c.colors = 'B'
  AND INSTR(',' || c.types || ',', ',Artifact,') > 0
  AND TRIM(f.flavorText) <> ''
LIMIT 1
