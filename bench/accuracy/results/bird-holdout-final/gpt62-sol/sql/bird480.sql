SELECT COUNT(*) AS carbon_carbon_double_bond_pairs
FROM (
    SELECT DISTINCT m.molecule_id,
           MIN(c.atom_id, c.atom_id2) AS atom_lo,
           MAX(c.atom_id, c.atom_id2) AS atom_hi
    FROM molecule AS m
    JOIN bond AS b ON b.molecule_id = m.molecule_id
    JOIN connected AS c ON c.bond_id = b.bond_id
    JOIN atom AS a1 ON a1.atom_id = c.atom_id AND a1.molecule_id = m.molecule_id
    JOIN atom AS a2 ON a2.atom_id = c.atom_id2 AND a2.molecule_id = m.molecule_id
    WHERE m.label = '+'
      AND b.bond_type = '='
      AND a1.element = 'c'
      AND a2.element = 'c'
) AS pairs;
