SELECT
    m.molecule_id,
    CASE m.label
        WHEN '+' THEN 'YES'
        WHEN '-' THEN 'NO'
    END AS carcinogenic,
    COUNT(b.bond_id) AS multiple_bond_count
FROM main.atom AS a
JOIN main.molecule AS m ON m.molecule_id = a.molecule_id
LEFT JOIN main.bond AS b
    ON b.molecule_id = m.molecule_id
    AND b.bond_type IN ('=', '#')
WHERE a.atom_id = 'TR041_10'
GROUP BY m.molecule_id, m.label
LIMIT 1
