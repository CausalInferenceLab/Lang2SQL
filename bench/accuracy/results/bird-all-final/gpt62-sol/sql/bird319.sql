SELECT e.type AS event_category,
       COUNT(DISTINCT m.member_id) AS member_count
FROM event AS e
LEFT JOIN attendance AS a ON a.link_to_event = e.event_id
LEFT JOIN member AS m ON m.member_id = a.link_to_member
WHERE e.location = 'MU 215'
GROUP BY e.type
ORDER BY e.type;
