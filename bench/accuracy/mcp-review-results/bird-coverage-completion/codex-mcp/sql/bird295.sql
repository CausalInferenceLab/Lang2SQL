WITH event_totals AS (
    SELECT b.link_to_event,
           SUM(CAST(ROUND(e.cost * 100) AS INTEGER)) AS total_cents
    FROM main.expense AS e
    JOIN main.member AS m ON m.member_id = e.link_to_member
    JOIN main.budget AS b ON b.budget_id = e.link_to_budget
    WHERE m.first_name = 'Elijah'
      AND m.last_name = 'Allen'
      AND strftime('%m', e.expense_date) IN ('09', '10')
      AND b.link_to_event IS NOT NULL
    GROUP BY b.link_to_event
)
SELECT ROUND(AVG(total_cents), 0) / 100.0 AS mean_total_expense_per_event
FROM event_totals
LIMIT 1
