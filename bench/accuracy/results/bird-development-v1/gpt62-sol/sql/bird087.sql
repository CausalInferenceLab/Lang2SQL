SELECT COUNT(*) AS total_official_rulings
FROM (
    SELECT DISTINCT r.date, r.text
    FROM rulings AS r
    WHERE r.uuid IN (
        SELECT c.uuid
        FROM cards AS c
        WHERE c.name = 'Fireball'
    )
) AS fireball_rulings;
