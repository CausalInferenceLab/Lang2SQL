SELECT (
    SELECT 1.0 * COUNT(CASE WHEN a.element = 'h' THEN 1 END)
           / NULLIF(COUNT(*), 0)
    FROM atom AS a
    WHERE a.molecule_id = m.molecule_id
) AS hydrogen_fraction,
       m.label AS carcinogenicity_label
FROM molecule AS m
WHERE m.molecule_id = 'TR006';
