SELECT ROUND(
    100.0 * COUNT(CASE WHEN instr(p.Tags, '<r>') > 0 THEN 1 END)
    / NULLIF(COUNT(*), 0),
    2
) AS r_tag_percentage
FROM main.posts AS p
JOIN main.users AS u ON u.Id = p.OwnerUserId
WHERE u.DisplayName = 'Community'
LIMIT 1
