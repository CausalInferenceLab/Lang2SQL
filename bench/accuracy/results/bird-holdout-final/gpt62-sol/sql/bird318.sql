WITH latest_vp_expense AS (
    SELECT MAX(e.expense_date) AS expense_date
    FROM expense AS e
    JOIN member AS m ON m.member_id = e.link_to_member
    WHERE m.position = 'Vice President'
)
SELECT e.expense_date, SUM(e.cost) AS total_cost
FROM expense AS e
JOIN latest_vp_expense AS v ON e.expense_date = v.expense_date
GROUP BY e.expense_date;
