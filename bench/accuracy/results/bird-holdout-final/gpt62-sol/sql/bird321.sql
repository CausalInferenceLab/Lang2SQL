SELECT e.location,
       COUNT(DISTINCT x.expense_description) AS distinct_things_bought
FROM event AS e
LEFT JOIN budget AS b ON b.link_to_event = e.event_id
LEFT JOIN expense AS x ON x.link_to_budget = b.budget_id
WHERE EXISTS (
    SELECT 1
    FROM budget AS qualifying_budget
    WHERE qualifying_budget.link_to_event = e.event_id
      AND qualifying_budget.amount > 40
)
GROUP BY e.location;
