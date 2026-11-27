WITH constructor_scores AS (
    SELECT cr.constructorId, SUM(cr.points) AS score
    FROM constructorResults AS cr
    JOIN races AS r ON r.raceId = cr.raceId
    WHERE r.name = 'Monaco Grand Prix'
      AND r.year BETWEEN 1980 AND 2010
    GROUP BY cr.constructorId
)
SELECT cs.score, c.name, c.nationality
FROM constructor_scores AS cs
JOIN constructors AS c ON c.constructorId = cs.constructorId
WHERE cs.score = (SELECT MAX(score) FROM constructor_scores);
