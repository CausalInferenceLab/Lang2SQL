WITH events_2019 AS (
    SELECT event_id, type AS event_type
    FROM event
    WHERE event_date >= '2019-01-01'
      AND event_date < '2020-01-01'
),
attendance_by_type AS (
    SELECT e.event_type, COUNT(DISTINCT a.link_to_member) AS members_attended
    FROM events_2019 AS e
    JOIN attendance AS a ON a.link_to_event = e.event_id
    GROUP BY e.event_type
),
budget_by_type AS (
    SELECT e.event_type, COALESCE(SUM(b.amount), 0) AS budgeted_dollars
    FROM events_2019 AS e
    LEFT JOIN budget AS b ON b.link_to_event = e.event_id
    GROUP BY e.event_type
)
SELECT b.event_type,
       COALESCE(a.members_attended, 0) AS members_attended,
       b.budgeted_dollars
FROM budget_by_type AS b
LEFT JOIN attendance_by_type AS a ON a.event_type IS b.event_type
ORDER BY b.event_type;
