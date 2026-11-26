WITH classified AS (
    SELECT m.label,
           NOT EXISTS (
               SELECT 1
               FROM bond AS b
               WHERE b.molecule_id = m.molecule_id
                 AND b.bond_type IS NOT '-'
           ) AS all_single_bonds
    FROM molecule AS m
)
SELECT COUNT(*) FILTER (WHERE all_single_bonds AND label = '+')
       - COUNT(*) FILTER (WHERE all_single_bonds AND label = '-') AS all_single_bond_difference,
       COUNT(*) FILTER (WHERE NOT all_single_bonds AND label = '+')
       - COUNT(*) FILTER (WHERE NOT all_single_bonds AND label = '-') AS other_molecule_difference
FROM classified;
