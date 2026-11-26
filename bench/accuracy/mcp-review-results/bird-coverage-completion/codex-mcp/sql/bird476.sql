SELECT COUNT(*) AS atom_count
FROM main.atom AS a
WHERE a.molecule_id IN (
    SELECT b.molecule_id
    FROM main.bond AS b
    WHERE b.bond_type = '#'
    INTERSECT
    SELECT p.molecule_id
    FROM main.atom AS p
    WHERE p.element IN ('p', 'br')
)
LIMIT 1
