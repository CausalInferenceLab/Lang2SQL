WITH member_attendance AS (
    SELECT
        m.member_id,
        CASE WHEN m.position IN ('President', 'Vice President', 'Secretary', 'Treasurer')
             THEN 1 ELSE 0 END AS is_officer,
        COUNT(DISTINCT e.event_id) AS events_attended
    FROM member AS m
    LEFT JOIN attendance AS a ON a.link_to_member = m.member_id
    LEFT JOIN event AS e ON e.event_id = a.link_to_event
    GROUP BY m.member_id, m.position
)
SELECT
    ROUND(AVG(CASE WHEN is_officer = 1 THEN events_attended END), 2) AS avg_events_per_officer,
    ROUND(AVG(CASE WHEN is_officer = 0 THEN events_attended END), 2) AS avg_events_per_other_student
FROM member_attendance;
