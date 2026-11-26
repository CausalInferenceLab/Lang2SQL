SELECT COUNT(DISTINCT b.bond_id) AS bond_count
FROM main.bond AS b
JOIN main.connected AS c ON c.bond_id = b.bond_id
JOIN main.atom AS h ON h.atom_id IN (c.atom_id, c.atom_id2)
WHERE h.element = 'h'
  AND b.molecule_id IN (
      SELECT y.molecule_id
      FROM main.atom AS y
      WHERE y.element = 'y'
  )
LIMIT 1
