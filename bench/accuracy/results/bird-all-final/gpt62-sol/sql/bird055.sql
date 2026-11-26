SELECT COUNT(DISTINCT c.uuid) AS banned_white_bordered_printings
FROM cards AS c
WHERE c.borderColor = 'white'
  AND c.uuid IN (
    SELECT l.uuid
    FROM legalities AS l
    WHERE l.status = 'Banned'
  );
