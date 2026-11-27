SELECT
    c.name AS constructor_name,
    r.year,
    ds.points + cs.points AS combined_points
FROM team_driver_ranks AS tdr
JOIN constructors AS c ON c.constructor_id = tdr.constructor_id
JOIN races AS r ON r.year = tdr.year
JOIN constructor_standings AS cs
    ON cs.race_id = r.race_id AND cs.constructor_id = tdr.constructor_id
JOIN driver_standings AS ds
    ON ds.race_id = r.race_id AND ds.driver_id = tdr.driver_id
WHERE tdr.team_driver_rank = 1
  AND r.round = (SELECT MAX(r2.round) FROM races AS r2 WHERE r2.year = r.year)
ORDER BY combined_points DESC, r.year DESC, c.constructor_id
LIMIT 3;
