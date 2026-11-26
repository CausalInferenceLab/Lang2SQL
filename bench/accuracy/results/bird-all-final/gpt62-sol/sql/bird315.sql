WITH attendee_counts AS (
    SELECT link_to_event AS event_id,
           COUNT(DISTINCT link_to_member) AS attendee_count
    FROM attendance
    GROUP BY link_to_event
    HAVING COUNT(DISTINCT link_to_member) > 0
), water_spending AS (
    SELECT b.link_to_event AS event_id,
           SUM(x.cost) AS water_cost
    FROM expense AS x
    JOIN budget AS b ON b.budget_id = x.link_to_budget
    WHERE LOWER(x.expense_description) LIKE '%water%'
    GROUP BY b.link_to_event
)
SELECT e.event_name,
       ROUND(1.0 * COALESCE(w.water_cost, 0.00) / a.attendee_count, 2) AS water_cost_per_attendee
FROM event AS e
JOIN attendee_counts AS a ON a.event_id = e.event_id
LEFT JOIN water_spending AS w ON w.event_id = e.event_id
ORDER BY e.event_name, e.event_id;
