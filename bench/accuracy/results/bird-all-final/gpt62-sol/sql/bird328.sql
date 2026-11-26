SELECT
    e.event_name,
    e.event_date,
    (
        SELECT SUM(b.amount)
        FROM budget AS b
        WHERE b.link_to_event = e.event_id
    ) AS total_budgeted,
    (
        SELECT COUNT(DISTINCT a.link_to_member)
        FROM attendance AS a
        WHERE a.link_to_event = e.event_id
    ) AS member_attendance_count
FROM event AS e
ORDER BY e.event_name ASC, e.event_date ASC;
