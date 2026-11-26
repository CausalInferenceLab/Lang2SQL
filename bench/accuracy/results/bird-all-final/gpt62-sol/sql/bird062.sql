SELECT COUNT(DISTINCT c.name) AS card_count
FROM cards AS c
JOIN legalities AS l ON l.uuid = c.uuid
WHERE l.format = 'brawl'
  AND l.status = 'Legal'
  AND c.leadershipSkills LIKE '%''brawl'': True%'
  AND instr(',' || c.types || ',', ',Creature,') > 0
  AND EXISTS (
      SELECT 1
      FROM cards AS p
      WHERE p.name = c.name AND p.isPromo = 1
  );
