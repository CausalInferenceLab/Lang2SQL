WITH RECURSIVE items(expense_id, item, rest) AS (
    SELECT e.expense_id, '', REPLACE(e.expense_description, 'Bakery - ', '') || ','
    FROM expense AS e
    WHERE EXISTS (
        SELECT 1
        FROM budget AS b
        WHERE b.budget_id = e.link_to_budget
          AND b.category = 'Food'
    )
    UNION ALL
    SELECT expense_id,
           TRIM(SUBSTR(rest, 1, INSTR(rest, ',') - 1)),
           SUBSTR(rest, INSTR(rest, ',') + 1)
    FROM items
    WHERE rest <> ''
)
SELECT MIN(item) AS item_name, COUNT(DISTINCT expense_id) AS record_count
FROM items
WHERE item <> '' AND item COLLATE NOCASE <> 'supplies'
GROUP BY item COLLATE NOCASE
ORDER BY record_count DESC, item_name COLLATE NOCASE ASC
LIMIT 1;
