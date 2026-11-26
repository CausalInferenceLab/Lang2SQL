WITH qualifying_molecules AS (
    SELECT a.molecule_id
    FROM main.atom AS a
    WHERE a.element = 'n'
    EXCEPT
    SELECT a.molecule_id
    FROM main.bond AS b
    JOIN main.connected AS c ON c.bond_id = b.bond_id
    JOIN main.atom AS a ON a.atom_id IN (c.atom_id, c.atom_id2)
    WHERE b.bond_type IN ('=', '#')
      AND a.element = 'n'
)
SELECT COUNT(molecule_id) AS molecule_count
FROM qualifying_molecules
LIMIT 1000
