WITH atom_counts AS (
    SELECT molecule_id,
           COUNT(*) AS atom_count,
           COUNT(CASE WHEN element = 'c' THEN 1 END) AS carbon_atom_count,
           COUNT(CASE WHEN element = 'cl' THEN 1 END) AS chlorine_atom_count
    FROM atom
    GROUP BY molecule_id
    HAVING COUNT(CASE WHEN element = 'c' THEN 1 END) > 0
), carbon_bonds AS (
    SELECT DISTINCT c.bond_id
    FROM connected AS c
    JOIN atom AS a
      ON a.atom_id IN (c.atom_id, c.atom_id2)
    WHERE a.element = 'c'
), bond_counts AS (
    SELECT b.molecule_id,
           COUNT(*) AS bond_count,
           COUNT(cb.bond_id) AS carbon_touching_bond_count,
           COUNT(CASE WHEN b.bond_type IN ('=', '#') THEN 1 END) AS multiple_bond_count
    FROM bond AS b
    LEFT JOIN carbon_bonds AS cb ON cb.bond_id = b.bond_id
    GROUP BY b.molecule_id
)
SELECT ROW_NUMBER() OVER (
           ORDER BY COALESCE(bc.carbon_touching_bond_count, 0) DESC,
                    ac.atom_count DESC,
                    m.molecule_id ASC
       ) AS rank_position,
       m.molecule_id,
       m.label AS carcinogenicity_label,
       ac.atom_count,
       COALESCE(bc.bond_count, 0) AS bond_count,
       ac.carbon_atom_count,
       ac.chlorine_atom_count,
       COALESCE(bc.carbon_touching_bond_count, 0) AS carbon_touching_bond_count,
       COALESCE(bc.multiple_bond_count, 0) AS multiple_bond_count
FROM molecule AS m
JOIN atom_counts AS ac ON ac.molecule_id = m.molecule_id
LEFT JOIN bond_counts AS bc ON bc.molecule_id = m.molecule_id
ORDER BY rank_position;
