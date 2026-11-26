SELECT SUM(b.amount) AS total_budgeted
FROM event AS e
JOIN budget AS b ON b.link_to_event = e.event_id
WHERE e.event_date >= '2019-09-01'
  AND e.event_date < '2019-10-01'
  AND EXISTS (
    SELECT 1
    FROM budget AS pizza_budget
    JOIN expense AS x ON x.link_to_budget = pizza_budget.budget_id
    WHERE pizza_budget.link_to_event = e.event_id
      AND x.expense_description = 'Pizza'
  );
