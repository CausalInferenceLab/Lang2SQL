WITH attendee_counts AS (
    SELECT a.link_to_event AS event_id, COUNT(*) AS attendee_count
    FROM main.attendance AS a
    WHERE a.link_to_member IS NOT NULL
    GROUP BY a.link_to_event
),
water_spending AS (
    SELECT b.link_to_event AS event_id,
           SUM(CAST(ROUND(x.cost * 100) AS INTEGER)) AS total_cents
    FROM main.expense AS x
    JOIN main.budget AS b ON b.budget_id = x.link_to_budget
    WHERE x.expense_description LIKE '%water%'
    GROUP BY b.link_to_event
)
SELECT e.event_name,
       ROUND(COALESCE(w.total_cents, 0) * 1.0 / a.attendee_count) / 100.0
           AS water_cost_per_attendee
FROM main.event AS e
JOIN attendee_counts AS a ON a.event_id = e.event_id
LEFT JOIN water_spending AS w ON w.event_id = e.event_id
ORDER BY e.event_name, e.event_id
LIMIT -1
