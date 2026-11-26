WITH event_budgets AS (
    SELECT b.link_to_event, SUM(b.amount) AS total_budgeted
    FROM budget AS b
    GROUP BY b.link_to_event
),
event_attendance AS (
    SELECT a.link_to_event, COUNT(a.link_to_member) AS members_attended
    FROM attendance AS a
    GROUP BY a.link_to_event
)
SELECT
    e.event_name,
    e.event_date,
    b.total_budgeted,
    COALESCE(a.members_attended, 0) AS members_attended
FROM event AS e
LEFT JOIN event_budgets AS b ON b.link_to_event = e.event_id
LEFT JOIN event_attendance AS a ON a.link_to_event = e.event_id
ORDER BY e.event_name ASC, e.event_date ASC
LIMIT 1000
