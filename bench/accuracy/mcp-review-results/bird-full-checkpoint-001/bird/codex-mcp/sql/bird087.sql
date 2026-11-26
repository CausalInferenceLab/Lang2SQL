WITH fireball_rulings AS (
    SELECT DISTINCT r.date, r.text
    FROM main.rulings AS r
    JOIN main.cards AS c ON c.uuid = r.uuid
    WHERE c.name = 'Fireball'
)
SELECT COUNT(*) AS total_rulings
FROM fireball_rulings
LIMIT 1
