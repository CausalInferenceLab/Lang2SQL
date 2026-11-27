SELECT ROUND(100.0 * AVG(CASE WHEN INSTR(p.Tags, '<r>') > 0 THEN 1 ELSE 0 END), 2) AS percentage
FROM posts AS p
JOIN users AS u ON u.Id = p.OwnerUserId
WHERE u.DisplayName = 'Community';
