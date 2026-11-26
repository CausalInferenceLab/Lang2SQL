WITH latest_expense AS (
    SELECT MAX(e.expense_date) AS expense_date
    FROM main.expense AS e
    JOIN main.member AS m ON m.member_id = e.link_to_member
    WHERE m.position = 'Vice President'
)
SELECT e.expense_date, SUM(e.cost) AS total_cost
FROM main.expense AS e
JOIN latest_expense AS latest ON latest.expense_date = e.expense_date
GROUP BY e.expense_date
LIMIT 1
