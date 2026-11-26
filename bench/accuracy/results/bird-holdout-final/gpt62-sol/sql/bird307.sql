SELECT SUM(i.amount) AS total_dues_dollars
FROM income AS i
WHERE i.source = 'Dues'
  AND EXISTS (
    SELECT 1
    FROM attendance AS a
    JOIN event AS e ON e.event_id = a.link_to_event
    WHERE a.link_to_member = i.link_to_member
      AND e.event_name = 'Football game'
  );
