SELECT COUNT(*) AS total_rulings
FROM (
    SELECT DISTINCT r.date, r.text
    FROM rulings AS r
    JOIN cards AS c ON c.uuid = r.uuid
    WHERE c.name = 'Fireball'
) AS fireball_rulings;
