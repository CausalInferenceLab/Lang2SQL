SELECT
    COUNT(DISTINCT CASE WHEN a.element = 'i' THEN a.atom_id END) AS iodine_atoms,
    COUNT(DISTINCT CASE WHEN a.element = 's' THEN a.atom_id END) AS sulfur_atoms
FROM main.bond AS b
JOIN main.connected AS c ON c.bond_id = b.bond_id
JOIN main.atom AS a ON a.atom_id IN (c.atom_id, c.atom_id2)
WHERE b.bond_type = '-'
  AND a.element IN ('i', 's')
LIMIT 1
