SELECT DISTINCT replace(c.text, char(10), ' ') AS rules_text
FROM main.cards AS c
WHERE c.name = 'Benalish Knight'
LIMIT 1000
