SELECT m.first_name, m.last_name
FROM member AS m
JOIN attendance AS a ON a.link_to_member = m.member_id
JOIN event AS e ON e.event_id = a.link_to_event
GROUP BY m.member_id, m.first_name, m.last_name
HAVING COUNT(DISTINCT e.event_id) > 7;
