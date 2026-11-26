WITH airport_points AS (
    SELECT airport_code,
           json_extract(city, '$.en') AS city_name,
           trim(coordinates, '()') AS point
    FROM airports_data
), airports AS (
    SELECT airport_code,
           city_name,
           radians(CAST(substr(point, 1, instr(point, ',') - 1) AS REAL)) AS lon,
           radians(CAST(substr(point, instr(point, ',') + 1) AS REAL)) AS lat
    FROM airport_points
), flight_distances AS (
    SELECT min(dep.city_name, arr.city_name) AS city1,
           max(dep.city_name, arr.city_name) AS city2,
           2 * 6371 * asin(sqrt(min(1.0,
               pow(sin((arr.lat - dep.lat) / 2), 2)
               + cos(dep.lat) * cos(arr.lat)
                 * pow(sin((arr.lon - dep.lon) / 2), 2)
           ))) AS distance_km
    FROM flights AS f
    JOIN airports AS dep ON dep.airport_code = f.departure_airport
    JOIN airports AS arr ON arr.airport_code = f.arrival_airport
    WHERE dep.city_name = 'Abakan' OR arr.city_name = 'Abakan'
), route_distances AS (
    SELECT AVG(distance_km) AS avg_distance_km
    FROM flight_distances
    GROUP BY city1, city2
)
SELECT MAX(avg_distance_km) AS longest_route_distance_km
FROM route_distances;
