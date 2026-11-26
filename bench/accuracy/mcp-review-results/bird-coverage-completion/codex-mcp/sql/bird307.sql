SELECT SUM(i.amount) AS total_dues_dollars
FROM main.income AS i
WHERE i.source = 'Dues'
  AND i.link_to_member IN (
      SELECT a.link_to_member
      FROM main.attendance AS a
      JOIN main.event AS e ON e.event_id = a.link_to_event
      WHERE e.event_name = 'Football game'
  )
LIMIT 1
