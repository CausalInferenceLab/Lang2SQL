WITH major_counts AS (
    SELECT mj.major_name, COUNT(DISTINCT m.member_id) AS attendee_count
    FROM attendance AS a
    JOIN event AS e ON e.event_id = a.link_to_event
    JOIN member AS m ON m.member_id = a.link_to_member
    JOIN major AS mj ON mj.major_id = m.link_to_major
    WHERE e.event_name = 'Football game'
    GROUP BY mj.major_id, mj.major_name
)
SELECT major_name, attendee_count
FROM major_counts
WHERE attendee_count = (SELECT MAX(attendee_count) FROM major_counts)
ORDER BY major_name ASC;
