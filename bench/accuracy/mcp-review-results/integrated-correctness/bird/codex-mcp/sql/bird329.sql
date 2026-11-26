WITH RECURSIVE descriptions(description) AS (
    SELECT CASE
               WHEN e.expense_description GLOB 'Bakery - *'
               THEN substr(e.expense_description, length('Bakery - ') + 1)
               ELSE e.expense_description
           END
    FROM main.expense AS e
    JOIN main.member AS m ON m.member_id = e.link_to_member
    WHERE m.first_name = 'Sacha'
      AND m.last_name = 'Harrison'
),
items(item_name, remaining) AS (
    SELECT '', description || ','
    FROM descriptions
    UNION ALL
    SELECT trim(substr(remaining, 1, instr(remaining, ',') - 1)),
           substr(remaining, instr(remaining, ',') + 1)
    FROM items
    WHERE remaining <> ''
)
SELECT item_name, COUNT(*) AS purchase_count
FROM items
WHERE item_name <> ''
GROUP BY item_name
ORDER BY item_name
LIMIT 1000
