SELECT
    e.location,
    COUNT(DISTINCT x.expense_description) AS distinct_things_bought
FROM event AS e
JOIN budget AS b ON b.link_to_event = e.event_id
LEFT JOIN expense AS x ON x.link_to_budget = b.budget_id
WHERE e.event_id IN (
    SELECT q.link_to_event
    FROM budget AS q
    WHERE q.amount > 40
)
GROUP BY e.location
LIMIT 1000
