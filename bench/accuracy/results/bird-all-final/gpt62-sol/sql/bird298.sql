SELECT 1.0 * SUM(CASE WHEN e.event_name = 'Yearly Kickoff' THEN b.amount END)
       / NULLIF(SUM(CASE WHEN e.event_name = 'October Meeting' THEN b.amount END), 0) AS advertisement_budget_ratio
FROM budget AS b
JOIN event AS e ON e.event_id = b.link_to_event
WHERE b.category = 'Advertisement'
  AND e.event_name IN ('Yearly Kickoff', 'October Meeting');
