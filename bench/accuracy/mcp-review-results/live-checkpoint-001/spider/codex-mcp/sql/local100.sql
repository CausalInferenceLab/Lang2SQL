WITH credits AS (
    SELECT MID, TRIM(PID) AS actor_id
    FROM main.M_Cast
),
khan_films AS (
    SELECT MID
    FROM credits
    WHERE actor_id IN (
        SELECT TRIM(PID)
        FROM main.Person
        WHERE TRIM(Name) = 'Shah Rukh Khan'
    )
),
khan_film_cast AS (
    SELECT DISTINCT actor_id
    FROM credits
    WHERE MID IN (SELECT MID FROM khan_films)
),
connected_films AS (
    SELECT DISTINCT MID
    FROM credits
    WHERE actor_id IN (SELECT actor_id FROM khan_film_cast)
),
two_steps_away AS (
    SELECT actor_id
    FROM credits
    WHERE MID IN (SELECT MID FROM connected_films)
    EXCEPT
    SELECT actor_id FROM khan_film_cast
)
SELECT COUNT(actor_id) AS actor_count
FROM two_steps_away
LIMIT 1
