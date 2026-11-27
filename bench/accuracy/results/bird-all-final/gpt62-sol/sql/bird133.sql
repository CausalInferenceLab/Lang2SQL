SELECT SUM(u.Reputation) AS combined_reputation
FROM users AS u
JOIN (
    SELECT DISTINCT p.OwnerUserId
    FROM posts AS p
    WHERE p.Score = 10
      AND instr(p.Tags, '<r>') > 0
) AS owners ON owners.OwnerUserId = u.Id;
