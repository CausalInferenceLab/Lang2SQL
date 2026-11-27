WITH rare_elements AS (
    SELECT element
    FROM atom
    WHERE element IS NOT NULL
    GROUP BY element
    HAVING COUNT(DISTINCT molecule_id) = 1
)
SELECT m.molecule_id, COUNT(DISTINCT b.bond_id) AS bond_count
FROM molecule AS m
JOIN atom AS a ON a.molecule_id = m.molecule_id
JOIN rare_elements AS r ON r.element = a.element
LEFT JOIN connected AS c ON c.atom_id = a.atom_id OR c.atom_id2 = a.atom_id
LEFT JOIN bond AS b ON b.bond_id = c.bond_id AND b.molecule_id = m.molecule_id
WHERE m.label = '+'
GROUP BY m.molecule_id
ORDER BY m.molecule_id;
