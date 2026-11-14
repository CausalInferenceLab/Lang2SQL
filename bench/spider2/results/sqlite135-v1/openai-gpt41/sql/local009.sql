WITH abakan_airports AS (
    SELECT airport_code
    FROM airports_data
    WHERE city = 'Abakan'
),
flights_abakan AS (
    SELECT
        f.flight_id,
        f.departure_airport AS from_airport,
        f.arrival_airport AS to_airport
    FROM flights f
    WHERE f.departure_airport IN (SELECT airport_code FROM abakan_airports)
       OR f.arrival_airport IN (SELECT airport_code FROM abakan_airports)
),
flight_cities AS (
    SELECT
        fa.flight_id,
        ad_from.city AS from_city,
        ad_from.longitude AS from_long,
        ad_from.latitude AS from_lat,
        ad_to.city AS to_city,
        ad_to.longitude AS to_long,
        ad_to.latitude AS to_lat
    FROM flights_abakan fa
    JOIN airports_data ad_from ON fa.from_airport = ad_from.airport_code
    JOIN airports_data ad_to ON fa.to_airport = ad_to.airport_code
),
city_pairs AS (
    SELECT
        flight_id,
        CASE WHEN from_city < to_city THEN from_city ELSE to_city END AS city1,
        CASE WHEN from_city < to_city THEN to_city ELSE from_city END AS city2,
        CASE WHEN from_city < to_city THEN from_long ELSE to_long END AS city1_long,
        CASE WHEN from_city < to_city THEN from_lat ELSE to_lat END AS city1_lat,
        CASE WHEN from_city < to_city THEN to_long ELSE from_long END AS city2_long,
        CASE WHEN from_city < to_city THEN to_lat ELSE from_lat END AS city2_lat
    FROM flight_cities
    WHERE from_city <> to_city -- Exclude flights within the same city
),
route_distances AS (
    SELECT
        city1,
        city2,
        2 * 6371 * ASIN(
            SQRT(
                POW(SIN(RADIANS((city2_lat - city1_lat)/2)), 2) +
                COS(RADIANS(city1_lat)) *
                COS(RADIANS(city2_lat)) *
                POW(SIN(RADIANS((city2_long - city1_long)/2)), 2)
            )
        ) AS distance
    FROM city_pairs
),
avg_route_distances AS (
    SELECT
        city1,
        city2,
        AVG(distance) AS avg_distance
    FROM route_distances
    GROUP BY city1, city2
)
SELECT
    MAX(avg_distance) AS longest_route_distance_km
FROM avg_route_distances
WHERE city1 = 'Abakan' OR city2 = 'Abakan';
