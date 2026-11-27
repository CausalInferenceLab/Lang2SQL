WITH qualifying_member_budgets AS (
    SELECT DISTINCT link_to_member, link_to_budget
    FROM expense
    WHERE cost > 50
)
SELECT m.first_name,
       m.last_name,
       ROUND(SUM(b.amount - b.spent), 2) AS total_amount_left
FROM qualifying_member_budgets AS q
JOIN member AS m ON m.member_id = q.link_to_member
JOIN budget AS b ON b.budget_id = q.link_to_budget
GROUP BY m.member_id, m.first_name, m.last_name
ORDER BY total_amount_left DESC, m.last_name ASC, m.first_name ASC;
