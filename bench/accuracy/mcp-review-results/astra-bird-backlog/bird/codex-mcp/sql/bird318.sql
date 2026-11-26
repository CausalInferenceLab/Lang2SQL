SELECT e.expense_date, SUM(e.cost) AS total_cost
FROM main.expense AS e
WHERE e.expense_date = (
    SELECT MAX(vp.expense_date)
    FROM main.expense AS vp
    JOIN main.member AS m ON m.member_id = vp.link_to_member
    WHERE m.position = 'Vice President'
)
GROUP BY e.expense_date
LIMIT 1
