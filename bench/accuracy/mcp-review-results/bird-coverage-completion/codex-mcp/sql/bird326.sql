WITH RECURSIVE items(expense_id, item, remaining) AS (
    SELECT e.expense_id, '', replace(e.expense_description, 'Bakery - ', '') || ','
    FROM main.expense AS e
    JOIN main.budget AS b ON b.budget_id = e.link_to_budget
    WHERE b.category = 'Food'

    UNION ALL

    SELECT expense_id,
           trim(substr(remaining, 1, instr(remaining, ',') - 1)),
           substr(remaining, instr(remaining, ',') + 1)
    FROM items
    WHERE remaining <> ''
)
SELECT min(item) AS item_name,
       count(DISTINCT expense_id) AS record_count
FROM items
WHERE item <> '' AND lower(item) <> 'supplies'
GROUP BY item COLLATE NOCASE
ORDER BY record_count DESC, item_name COLLATE NOCASE ASC
LIMIT 1
