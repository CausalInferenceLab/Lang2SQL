SELECT SUM(q.Score) AS total_score
FROM posts AS q
WHERE q.PostTypeId = 1
  AND q.Tags LIKE '%<r>%'
  AND EXISTS (
    SELECT 1
    FROM postLinks AS l
    JOIN posts AS target ON target.Id = l.RelatedPostId
    WHERE l.PostId = q.Id
      AND target.PostTypeId = 1
      AND target.Title = 'A Probability distribution value exceeding 1 is OK?'
  );
