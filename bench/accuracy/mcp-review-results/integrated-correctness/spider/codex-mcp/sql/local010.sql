WITH airport_coordinates AS (
    SELECT airport_code,
           json_extract(city, '$.en') AS city,
           radians(CAST(substr(coordinates, 2, instr(coordinates, ',') - 2) AS REAL)) AS longitude,
           radians(CAST(rtrim(substr(coordinates, instr(coordinates, ',') + 1), ')') AS REAL)) AS latitude
    FROM main.airports_data
),
flight_distances AS (
    SELECT MIN(departure.city, arrival.city) AS city1,
           MAX(departure.city, arrival.city) AS city2,
           2.0 * 6371.0 * asin(sqrt(MIN(1.0,
               pow(sin((arrival.latitude - departure.latitude) / 2.0), 2)
               + cos(departure.latitude) * cos(arrival.latitude)
                 * pow(sin((arrival.longitude - departure.longitude) / 2.0), 2)
           ))) AS distance_km
    FROM main.flights AS flight
    JOIN airport_coordinates AS departure ON departure.airport_code = flight.departure_airport
    JOIN airport_coordinates AS arrival ON arrival.airport_code = flight.arrival_airport
),
city_pair_distances AS (
    SELECT city1, city2, AVG(distance_km) AS average_distance_km
    FROM flight_distances
    GROUP BY city1, city2
),
distance_ranges (lower_km, upper_km) AS (
    VALUES (0, 1000), (1000, 2000), (2000, 3000), (3000, 4000),
           (4000, 5000), (5000, 6000), (6000, NULL)
),
range_counts AS (
    SELECT distance_ranges.lower_km, COUNT(city_pair_distances.average_distance_km) AS pair_count
    FROM distance_ranges
    LEFT JOIN city_pair_distances
      ON city_pair_distances.average_distance_km >= distance_ranges.lower_km
     AND (city_pair_distances.average_distance_km < distance_ranges.upper_km
          OR distance_ranges.upper_km IS NULL)
    GROUP BY distance_ranges.lower_km
)
SELECT pair_count
FROM range_counts
ORDER BY pair_count
LIMIT 1
