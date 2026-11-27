WITH RECURSIVE
period_expenses AS (
    SELECT link_to_member AS member_id, expense_description, cost
    FROM expense
    WHERE expense_date >= '2019-10-01'
      AND expense_date < '2019-12-01'
),
item_parts(member_id, item, rest) AS (
    SELECT member_id, NULL, expense_description || ','
    FROM period_expenses
    UNION ALL
    SELECT member_id,
           SUBSTR(rest, 1, INSTR(rest, ',') - 1),
           SUBSTR(rest, INSTR(rest, ',') + 1)
    FROM item_parts
    WHERE rest <> ''
),
item_counts AS (
    SELECT member_id, COUNT(DISTINCT NULLIF(LOWER(TRIM(item)), '')) AS kind_count
    FROM item_parts
    GROUP BY member_id
),
spending AS (
    SELECT member_id, ROUND(SUM(cost), 2) AS total_spending_dollars
    FROM period_expenses
    GROUP BY member_id
),
ranked AS (
    SELECT m.first_name || ' ' || m.last_name AS full_name,
           i.kind_count,
           s.total_spending_dollars,
           RANK() OVER (ORDER BY i.kind_count DESC) AS kind_rank
    FROM member AS m
    JOIN item_counts AS i ON i.member_id = m.member_id
    JOIN spending AS s ON s.member_id = m.member_id
)
SELECT full_name, kind_count, total_spending_dollars
FROM ranked
WHERE kind_rank = 1
ORDER BY full_name;
