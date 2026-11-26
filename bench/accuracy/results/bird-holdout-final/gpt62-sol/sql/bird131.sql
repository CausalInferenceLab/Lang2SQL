WITH qualifying_owners AS (
    SELECT DISTINCT OwnerUserId
    FROM posts
    WHERE Score < 20
      AND INSTR(Tags, '<r>') > 0
)
SELECT CAST(ROUND(AVG(u.Reputation)) AS INTEGER) AS average_reputation
FROM qualifying_owners AS q
JOIN users AS u ON u.Id = q.OwnerUserId;
