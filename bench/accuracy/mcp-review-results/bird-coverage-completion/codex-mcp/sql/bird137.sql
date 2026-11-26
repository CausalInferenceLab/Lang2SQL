WITH california_users AS (
    SELECT Id, DisplayName, Views, Reputation
    FROM main.users
    WHERE (' ' || lower(Location) || ' ') GLOB '*[^a-z0-9]ca[^a-z0-9]*'
       OR (
           (' ' || lower(Location) || ' ') GLOB '*[^a-z0-9]california[^a-z0-9]*'
           AND lower(trim(Location)) <> 'california, south africa'
       )
)
SELECT Id, DisplayName, Views, Reputation
FROM california_users
WHERE Views = (SELECT MAX(Views) FROM california_users)
ORDER BY Id
LIMIT -1
