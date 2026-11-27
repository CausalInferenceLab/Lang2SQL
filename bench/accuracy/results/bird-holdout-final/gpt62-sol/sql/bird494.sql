WITH noncarcinogenic_atoms AS (
    SELECT a.atom_id, a.molecule_id, a.element
    FROM atom AS a
    JOIN molecule AS m ON m.molecule_id = a.molecule_id
    WHERE m.label = '-'
), qualifying_bonds AS (
    SELECT DISTINCT b.molecule_id, b.bond_id,
           CASE WHEN a1.element = 'c' THEN a2.element ELSE a1.element END AS element
    FROM bond AS b
    JOIN connected AS c ON c.bond_id = b.bond_id
    JOIN noncarcinogenic_atoms AS a1 ON a1.atom_id = c.atom_id AND a1.molecule_id = b.molecule_id
    JOIN noncarcinogenic_atoms AS a2 ON a2.atom_id = c.atom_id2 AND a2.molecule_id = b.molecule_id
    WHERE b.bond_type IN ('=', '#')
      AND (a1.element = 'c' OR a2.element = 'c')
)
SELECT e.element, COUNT(q.bond_id) AS bond_count
FROM (SELECT DISTINCT element FROM noncarcinogenic_atoms) AS e
LEFT JOIN qualifying_bonds AS q ON q.element = e.element
GROUP BY e.element
ORDER BY e.element;
