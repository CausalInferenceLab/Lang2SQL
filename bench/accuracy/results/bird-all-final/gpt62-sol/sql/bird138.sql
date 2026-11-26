WITH york_users AS (
    SELECT Age, Reputation
    FROM users
    WHERE lower(trim(Location)) IN (
        'york', 'york, england', 'york, united kingdom', 'york, uk',
        'york, england, united kingdom', 'york, england, uk'
    )
)
SELECT Age
FROM york_users
WHERE Reputation = (SELECT MAX(Reputation) FROM york_users);
