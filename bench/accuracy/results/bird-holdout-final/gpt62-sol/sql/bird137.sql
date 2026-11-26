WITH california_users AS (
    SELECT Id, DisplayName, Views, Reputation
    FROM users
    WHERE (' ' || LOWER(Location) || ' ') GLOB '*[^a-z]california[^a-z]*'
       OR (' ' || LOWER(Location) || ' ') GLOB '*[^a-z]ca[^a-z]*'
)
SELECT Id, DisplayName, Views, Reputation
FROM california_users
WHERE Views = (SELECT MAX(Views) FROM california_users)
ORDER BY Id;
