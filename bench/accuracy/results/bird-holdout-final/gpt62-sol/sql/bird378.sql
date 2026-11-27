SELECT ROUND(100.0 * COUNT(CASE WHEN a.alignment = 'Good' THEN 1 END) / COUNT(*), 2) AS good_percentage
FROM superhero AS s
JOIN publisher AS p ON p.id = s.publisher_id
LEFT JOIN alignment AS a ON a.id = s.alignment_id
WHERE p.publisher_name = 'Marvel Comics'
  AND INSTR(TRIM(s.full_name), ' ') > 0
  AND TRIM(s.full_name) NOT IN ('Howard (Last name unrevealed)', 'Weapon XI');
