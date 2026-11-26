WITH event_attendance AS (
    SELECT e.event_id, COUNT(a.link_to_member) AS attendees
    FROM main.event AS e
    LEFT JOIN main.attendance AS a ON a.link_to_event = e.event_id
    WHERE e.event_id IN (
        SELECT b.link_to_event
        FROM main.budget AS b
        WHERE b.category = 'Advertisement'
    )
    GROUP BY e.event_id
)
SELECT ROUND(AVG(attendees), 2) AS average_attendance
FROM event_attendance
LIMIT 1
