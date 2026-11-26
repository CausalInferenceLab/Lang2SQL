WITH airports AS (
    SELECT airport_code,
           json_extract(city, '$.en') AS city_name,
           radians(CAST(substr(coordinates, 2, instr(coordinates, ',') - 2) AS REAL)) AS lon,
           radians(CAST(rtrim(substr(coordinates, instr(coordinates, ',') + 1), ')') AS REAL)) AS lat
    FROM main.airports_data
),
flight_distances AS (
    SELECT min(d.city_name, a.city_name) AS city1,
           max(d.city_name, a.city_name) AS city2,
           2 * 6371.0 * asin(sqrt(min(1.0,
               pow(sin((a.lat - d.lat) / 2.0), 2)
               + cos(d.lat) * cos(a.lat) * pow(sin((a.lon - d.lon) / 2.0), 2)
           ))) AS distance_km
    FROM main.flights AS f
    JOIN airports AS d ON d.airport_code = f.departure_airport
    JOIN airports AS a ON a.airport_code = f.arrival_airport
),
pair_distances AS (
    SELECT city1, city2, avg(distance_km) AS avg_km
    FROM flight_distances
    GROUP BY city1, city2
),
ranges(lower_km) AS (
    VALUES (0), (1000), (2000), (3000), (4000), (5000), (6000)
),
range_counts AS (
    SELECT r.lower_km, count(p.avg_km) AS pair_count
    FROM ranges AS r
    LEFT JOIN pair_distances AS p
      ON p.avg_km >= r.lower_km
     AND (r.lower_km = 6000 OR p.avg_km < r.lower_km + 1000)
    GROUP BY r.lower_km
)
SELECT min(pair_count) AS fewest_city_pairs
FROM range_counts
LIMIT 1000
