SELECT
    e.event_name,
    e.location,
    COUNT(DISTINCT a.link_to_member) AS members_attended
FROM event AS e
LEFT JOIN attendance AS a ON a.link_to_event = e.event_id
WHERE e.type = 'Budget'
GROUP BY e.event_id, e.event_name, e.location;
