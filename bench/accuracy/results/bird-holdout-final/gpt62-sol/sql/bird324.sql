WITH budget_totals AS (
    SELECT link_to_event AS event_id,
           SUM(amount) AS total_budget,
           SUM(spent) AS total_spent
    FROM budget
    GROUP BY link_to_event
), purchase_totals AS (
    SELECT b.link_to_event AS event_id,
           SUM(CASE
                   WHEN NULLIF(TRIM(x.expense_description), '') IS NULL THEN 0
                   ELSE 1 + LENGTH(x.expense_description)
                          - LENGTH(REPLACE(x.expense_description, ',', ''))
               END) AS purchase_count
    FROM expense AS x
    JOIN budget AS b ON b.budget_id = x.link_to_budget
    GROUP BY b.link_to_event
)
SELECT e.event_name
FROM event AS e
LEFT JOIN budget_totals AS bt ON bt.event_id = e.event_id
LEFT JOIN purchase_totals AS pt ON pt.event_id = e.event_id
WHERE e.status = 'Closed'
  AND COALESCE(bt.total_spent, 0) <= COALESCE(bt.total_budget, 0)
ORDER BY COALESCE(pt.purchase_count, 0) DESC, e.event_id ASC
LIMIT 1;
