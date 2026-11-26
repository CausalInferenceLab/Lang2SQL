SELECT SUM(q.Score) AS total_score
FROM posts AS q
WHERE q.PostTypeId = 1
  AND instr(q.Tags, '<r>') > 0
  AND q.Id IN (
      SELECT link.PostId
      FROM postLinks AS link
      JOIN posts AS target ON target.Id = link.RelatedPostId
      WHERE target.PostTypeId = 1
        AND target.Title = 'A Probability distribution value exceeding 1 is OK?'
  )
LIMIT 1
