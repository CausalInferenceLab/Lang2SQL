WITH RECURSIVE period_expenses AS (
    SELECT e.link_to_member AS member_id, e.expense_description, e.cost
    FROM main.expense AS e
    WHERE e.expense_date >= '2019-10-01'
      AND e.expense_date < '2019-12-01'
),
items(member_id, item_name, remaining) AS (
    SELECT member_id, NULL, expense_description || ','
    FROM period_expenses
    UNION ALL
    SELECT member_id,
           LOWER(TRIM(SUBSTR(remaining, 1, INSTR(remaining, ',') - 1))),
           SUBSTR(remaining, INSTR(remaining, ',') + 1)
    FROM items
    WHERE remaining <> ''
),
kind_counts AS (
    SELECT member_id, COUNT(DISTINCT item_name) AS kind_count
    FROM items
    WHERE item_name <> ''
    GROUP BY member_id
),
spending AS (
    SELECT member_id, ROUND(SUM(cost), 2) AS total_spending_dollars
    FROM period_expenses
    GROUP BY member_id
)
SELECT m.first_name || ' ' || m.last_name AS full_name,
       k.kind_count,
       s.total_spending_dollars
FROM kind_counts AS k
JOIN main.member AS m ON m.member_id = k.member_id
JOIN spending AS s ON s.member_id = k.member_id
WHERE k.kind_count = (SELECT MAX(kind_count) FROM kind_counts)
ORDER BY full_name, m.member_id
LIMIT -1
