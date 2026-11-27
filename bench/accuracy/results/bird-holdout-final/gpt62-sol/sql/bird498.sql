WITH triple_bond_elements AS (
    SELECT DISTINCT a.element
    FROM bond AS b
    JOIN connected AS c ON c.bond_id = b.bond_id
    JOIN atom AS a ON a.atom_id IN (c.atom_id, c.atom_id2)
    WHERE b.bond_type = '#'
)
SELECT a.element AS element_symbol,
       COUNT(DISTINCT m.molecule_id) AS molecule_count
FROM triple_bond_elements AS t
JOIN atom AS a ON a.element = t.element
JOIN molecule AS m ON m.molecule_id = a.molecule_id
GROUP BY a.element
ORDER BY molecule_count DESC, element_symbol ASC;
