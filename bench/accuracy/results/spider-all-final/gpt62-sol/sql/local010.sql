WITH airport_coords AS (
    SELECT airport_code,
           json_extract(city, '$.en') AS city_name,
           radians(CAST(substr(coordinates, 2, instr(coordinates, ',') - 2) AS REAL)) AS lon,
           radians(CAST(substr(coordinates, instr(coordinates, ',') + 1,
                               length(coordinates) - instr(coordinates, ',') - 1) AS REAL)) AS lat
    FROM airports_data
), flight_haversines AS (
    SELECT min(d.city_name, a.city_name) AS city1,
           max(d.city_name, a.city_name) AS city2,
           pow(sin((a.lat - d.lat) / 2.0), 2)
           + cos(d.lat) * cos(a.lat) * pow(sin((a.lon - d.lon) / 2.0), 2) AS h
    FROM flights AS f
    JOIN airport_coords AS d ON d.airport_code = f.departure_airport
    JOIN airport_coords AS a ON a.airport_code = f.arrival_airport
), city_pair_distances AS (
    SELECT AVG(2.0 * 6371 * asin(sqrt(min(1.0, h)))) AS avg_km
    FROM flight_haversines
    GROUP BY city1, city2
), distance_ranges(lower_km, upper_km) AS (
    VALUES (0, 1000), (1000, 2000), (2000, 3000), (3000, 4000),
           (4000, 5000), (5000, 6000), (6000, NULL)
), range_counts AS (
    SELECT COUNT(p.avg_km) AS pair_count
    FROM distance_ranges AS r
    LEFT JOIN city_pair_distances AS p
      ON p.avg_km >= r.lower_km
     AND (p.avg_km < r.upper_km OR r.upper_km IS NULL)
    GROUP BY r.lower_km
)
SELECT MIN(pair_count) AS fewest_unique_city_pairs
FROM range_counts;
