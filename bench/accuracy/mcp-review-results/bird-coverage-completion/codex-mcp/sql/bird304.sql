WITH events_2019 AS (
    SELECT e.event_id, e.type AS event_type
    FROM main.event AS e
    WHERE e.event_date >= '2019-01-01'
      AND e.event_date < '2020-01-01'
),
attendance_by_type AS (
    SELECT e.event_type, COUNT(DISTINCT a.link_to_member) AS members_attended
    FROM events_2019 AS e
    LEFT JOIN main.attendance AS a ON a.link_to_event = e.event_id
    GROUP BY e.event_type
),
budget_by_type AS (
    SELECT e.event_type, COALESCE(SUM(b.amount), 0) AS budgeted_dollars
    FROM events_2019 AS e
    LEFT JOIN main.budget AS b ON b.link_to_event = e.event_id
    GROUP BY e.event_type
)
SELECT a.event_type, a.members_attended, b.budgeted_dollars
FROM attendance_by_type AS a
JOIN budget_by_type AS b ON b.event_type IS a.event_type
ORDER BY a.event_type
LIMIT -1
