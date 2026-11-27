WITH italian_constructors AS (
    SELECT constructorId, name, url
    FROM constructors
    WHERE nationality = 'Italian'
), season_standings AS (
    SELECT cs.constructorId,
           cs.points,
           ROW_NUMBER() OVER (
               PARTITION BY cs.constructorId, r.year
               ORDER BY r.round DESC, r.raceId DESC
           ) AS season_row
    FROM constructorStandings AS cs
    JOIN races AS r ON r.raceId = cs.raceId
    JOIN italian_constructors AS c ON c.constructorId = cs.constructorId
), career_totals AS (
    SELECT c.constructorId,
           c.name,
           c.url,
           COALESCE(SUM(s.points), 0.0) AS career_points
    FROM italian_constructors AS c
    LEFT JOIN season_standings AS s
        ON s.constructorId = c.constructorId AND s.season_row = 1
    GROUP BY c.constructorId, c.name, c.url
), ranked AS (
    SELECT name,
           career_points,
           url,
           RANK() OVER (ORDER BY career_points DESC) AS career_rank
    FROM career_totals
)
SELECT name AS constructor_name,
       career_points,
       url AS introduction_website
FROM ranked
WHERE career_rank <= 3
ORDER BY career_rank, constructor_name;
