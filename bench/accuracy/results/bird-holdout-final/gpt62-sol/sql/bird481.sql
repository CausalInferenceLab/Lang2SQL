WITH positive_atoms AS (
    SELECT a.atom_id, a.element
    FROM atom AS a
    WHERE EXISTS (
        SELECT 1
        FROM molecule AS m
        WHERE m.molecule_id = a.molecule_id AND m.label = '+'
    )
), bonded_atom_ids AS (
    SELECT c.atom_id
    FROM connected AS c
    JOIN bond AS b ON b.bond_id = c.bond_id
    UNION
    SELECT c.atom_id2
    FROM connected AS c
    JOIN bond AS b ON b.bond_id = c.bond_id
), element_counts AS (
    SELECT p.element,
           COUNT(*) AS atom_count,
           COUNT(b.atom_id) AS bonded_atom_count
    FROM positive_atoms AS p
    LEFT JOIN bonded_atom_ids AS b ON b.atom_id = p.atom_id
    GROUP BY p.element
)
SELECT element,
       ROUND(100.0 * atom_count / (SELECT SUM(atom_count) FROM element_counts), 2) AS percentage
FROM element_counts
WHERE bonded_atom_count > 0
  AND bonded_atom_count = (SELECT MAX(bonded_atom_count) FROM element_counts)
ORDER BY element;
