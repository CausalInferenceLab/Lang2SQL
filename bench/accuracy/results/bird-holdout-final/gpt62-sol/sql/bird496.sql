SELECT DISTINCT m.molecule_id
FROM molecule AS m
WHERE m.label = '-'
  AND EXISTS (
    SELECT 1
    FROM atom AS a
    WHERE a.molecule_id = m.molecule_id
      AND a.element IN ('na', 'k', 'ca', 'cu', 'zn', 'sn', 'pb', 'mg', 'li')
  )
  AND EXISTS (
    SELECT 1
    FROM bond AS b
    WHERE b.molecule_id = m.molecule_id
      AND b.bond_type = '-'
  )
ORDER BY m.molecule_id ASC
LIMIT 3;
