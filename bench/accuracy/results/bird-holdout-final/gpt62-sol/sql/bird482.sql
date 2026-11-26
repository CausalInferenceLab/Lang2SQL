SELECT
    m.molecule_id,
    CASE m.label WHEN '+' THEN 'YES' WHEN '-' THEN 'NO' END AS carcinogenic,
    (
        SELECT COUNT(*)
        FROM bond AS b
        WHERE b.molecule_id = m.molecule_id
          AND b.bond_type IN ('=', '#')
    ) AS multiple_bond_count
FROM molecule AS m
WHERE EXISTS (
    SELECT 1
    FROM atom AS a
    WHERE a.atom_id = 'TR041_10'
      AND a.molecule_id = m.molecule_id
);
