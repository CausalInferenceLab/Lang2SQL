WITH ranked_drivers AS (
    SELECT r.year, ds.driver_id,
           RANK() OVER (
               PARTITION BY r.year
               ORDER BY r.round DESC, ds.points DESC
           ) AS points_rank
    FROM races AS r
    JOIN driver_standings AS ds ON ds.race_id = r.race_id
), ranked_constructors AS (
    SELECT r.year, cs.constructor_id,
           RANK() OVER (
               PARTITION BY r.year
               ORDER BY r.round DESC, cs.points DESC
           ) AS points_rank
    FROM races AS r
    JOIN constructor_standings AS cs ON cs.race_id = r.race_id
)
SELECT s.year,
       d.full_name AS driver_full_name,
       c.name AS constructor_name
FROM seasons AS s
LEFT JOIN ranked_drivers AS rd ON rd.year = s.year AND rd.points_rank = 1
LEFT JOIN drivers AS d ON d.driver_id = rd.driver_id
LEFT JOIN ranked_constructors AS rc ON rc.year = s.year AND rc.points_rank = 1
LEFT JOIN constructors AS c ON c.constructor_id = rc.constructor_id
ORDER BY s.year, d.full_name, c.name;
