WITH bond_types(bond_type, sort_order) AS (
    VALUES ('-', 1), ('=', 2), ('#', 3)
),
carbon_carbon_bonds AS (
    SELECT b.bond_id, b.bond_type
    FROM main.bond AS b
    JOIN main.molecule AS m ON m.molecule_id = b.molecule_id
    JOIN main.connected AS c ON c.bond_id = b.bond_id
    JOIN main.atom AS a1 ON a1.atom_id = c.atom_id
    JOIN main.atom AS a2 ON a2.atom_id = c.atom_id2
    WHERE m.label = '+'
      AND a1.element = 'c'
      AND a2.element = 'c'
)
SELECT t.bond_type, COUNT(DISTINCT b.bond_id) AS bond_count
FROM bond_types AS t
LEFT JOIN carbon_carbon_bonds AS b ON b.bond_type = t.bond_type
GROUP BY t.bond_type, t.sort_order
ORDER BY t.sort_order
LIMIT 1000
