SELECT ROUND(100.0 * COUNT(CASE WHEN NOT EXISTS (
    SELECT 1
    FROM atom AS a
    WHERE a.molecule_id = m.molecule_id AND a.element = 'f'
) THEN 1 END) / COUNT(*), 2) AS percentage
FROM molecule AS m
WHERE m.label = '+';
