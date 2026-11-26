WITH event_totals AS (
    SELECT v.event_id, SUM(e.cost) AS total_expense
    FROM expense AS e
    JOIN member AS m ON m.member_id = e.link_to_member
    JOIN budget AS b ON b.budget_id = e.link_to_budget
    JOIN event AS v ON v.event_id = b.link_to_event
    WHERE m.first_name = 'Elijah'
      AND m.last_name = 'Allen'
      AND strftime('%m', e.expense_date) IN ('09', '10')
    GROUP BY v.event_id
)
SELECT ROUND(AVG(total_expense), 2) AS mean_total_expense_per_event
FROM event_totals;
