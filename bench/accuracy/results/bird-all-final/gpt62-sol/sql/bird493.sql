WITH bond_endpoints AS (
  SELECT c.atom_id, c.bond_id
  FROM connected AS c
  JOIN bond AS b ON b.bond_id = c.bond_id
  WHERE b.molecule_id = 'TR397'
  UNION
  SELECT c.atom_id2 AS atom_id, c.bond_id
  FROM connected AS c
  JOIN bond AS b ON b.bond_id = c.bond_id
  WHERE b.molecule_id = 'TR397'
), atom_degrees AS (
  SELECT COUNT(e.bond_id) AS bond_count
  FROM atom AS a
  LEFT JOIN bond_endpoints AS e ON e.atom_id = a.atom_id
  WHERE a.molecule_id = 'TR397'
  GROUP BY a.atom_id
)
SELECT ROUND(AVG(bond_count), 2) AS average_bonds_per_atom
FROM atom_degrees;
