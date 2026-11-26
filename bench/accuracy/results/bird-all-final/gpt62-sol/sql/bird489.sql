SELECT
    ROUND(100.0 * COUNT(CASE WHEN m.label = '+' THEN 1 END) / COUNT(*), 2) AS carcinogenic_percent,
    ROUND(100.0 * COUNT(CASE WHEN m.label = '-' THEN 1 END) / COUNT(*), 2) AS non_carcinogenic_percent
FROM molecule AS m
WHERE m.label IN ('+', '-')
  AND EXISTS (
      SELECT 1
      FROM bond AS b
      WHERE b.molecule_id = m.molecule_id
        AND b.bond_type = '#'
  );
