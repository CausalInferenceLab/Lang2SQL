WITH molecule_bonds AS (
    SELECT b.bond_id, b.molecule_id, b.bond_type, m.label
    FROM bond AS b
    JOIN molecule AS m ON m.molecule_id = b.molecule_id
    WHERE m.molecule_id = 'TR377'
      AND b.bond_type IN ('-', '=')
), element_pairs AS (
    SELECT DISTINCT mb.bond_type,
           MIN(a1.element, a2.element) AS element_1,
           MAX(a1.element, a2.element) AS element_2
    FROM molecule_bonds AS mb
    JOIN connected AS c ON c.bond_id = mb.bond_id
    JOIN atom AS a1 ON a1.atom_id = c.atom_id AND a1.molecule_id = mb.molecule_id
    JOIN atom AS a2 ON a2.atom_id = c.atom_id2 AND a2.molecule_id = mb.molecule_id
    WHERE a1.element IS NOT NULL AND a2.element IS NOT NULL
)
SELECT mb.bond_type AS bond_kind,
       COUNT(DISTINCT mb.bond_id) AS bond_count,
       (SELECT COUNT(*) FROM element_pairs AS ep WHERE ep.bond_type = mb.bond_type) AS element_pair_count,
       mb.label AS carcinogenicity_label
FROM molecule_bonds AS mb
GROUP BY mb.bond_type, mb.label
ORDER BY bond_count DESC, bond_kind ASC;
