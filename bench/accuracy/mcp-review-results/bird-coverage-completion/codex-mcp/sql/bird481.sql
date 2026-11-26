WITH bond_endpoints AS (
    SELECT c.atom_id, c.atom_id2
    FROM main.connected AS c
    JOIN main.bond AS b ON b.bond_id = c.bond_id
),
bonded_atoms AS (
    SELECT atom_id FROM bond_endpoints
    UNION
    SELECT atom_id2 FROM bond_endpoints
),
element_counts AS (
    SELECT a.element,
           COUNT(*) AS atom_count,
           COUNT(ba.atom_id) AS bonded_count
    FROM main.atom AS a
    JOIN main.molecule AS m ON m.molecule_id = a.molecule_id
    LEFT JOIN bonded_atoms AS ba ON ba.atom_id = a.atom_id
    WHERE m.label = '+'
    GROUP BY a.element
),
ranked_elements AS (
    SELECT element, atom_count, bonded_count,
           SUM(atom_count) OVER () AS total_atoms,
           MAX(bonded_count) OVER () AS max_bonded_count
    FROM element_counts
)
SELECT element,
       ROUND(100.0 * atom_count / total_atoms, 2) AS percentage
FROM ranked_elements
WHERE bonded_count = max_bonded_count AND bonded_count > 0
ORDER BY element
LIMIT -1
