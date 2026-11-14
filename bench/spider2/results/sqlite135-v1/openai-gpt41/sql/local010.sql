WITH airport_cities AS (
  SELECT airport_code, city, longitude, latitude
  FROM airports_data
),
flight_city_routes AS (
  -- Assign city names to each flight's departure and arrival
  SELECT 
    f.flight_id,
    dep.city AS from_city,
    dep.longitude AS from_long,
    dep.latitude AS from_lat,
    arr.city AS to_city,
    arr.longitude AS to_long,
    arr.latitude AS to_lat
  FROM flights f
  JOIN airport_cities dep ON f.departure_airport = dep.airport_code
  JOIN airport_cities arr ON f.arrival_airport = arr.airport_code
  WHERE dep.city IS NOT NULL AND arr.city IS NOT NULL AND dep.city != arr.city
),
normalized_routes AS (
  -- Standardize city pair order by lexicographical comparison
  SELECT
    CASE WHEN from_city < to_city THEN from_city ELSE to_city END AS city1,
    CASE WHEN from_city < to_city THEN to_city ELSE from_city END AS city2,
    CASE WHEN from_city < to_city THEN from_long ELSE to_long END AS long1,
    CASE WHEN from_city < to_city THEN from_lat ELSE to_lat END AS lat1,
    CASE WHEN from_city < to_city THEN to_long ELSE from_long END AS long2,
    CASE WHEN from_city < to_city THEN to_lat ELSE from_lat END AS lat2
  FROM flight_city_routes
),
route_distances AS (
  -- Calculate the Haversine distance for each flight route between a city pair
  SELECT
    city1,
    city2,
    2 * 6371 *
      ASIN(
        MIN(1, -- SQLite asin input domain is [-1,1], so clamp just in case of floating point roundoff
          SQRT(
            POW(SIN((lat2 - lat1)*PI()/180/2), 2) +
              COS(lat1*PI()/180) * COS(lat2*PI()/180) *
              POW(SIN((long2 - long1)*PI()/180/2), 2)
          )
        )
      ) AS distance
  FROM normalized_routes
),
avg_pair_distances AS (
  -- Average distance for each unique city pair
  SELECT
    city1,
    city2,
    AVG(distance) AS avg_distance
  FROM route_distances
  GROUP BY city1, city2
),
binned_pairs AS (
  -- Assign each city pair to a distance "bin"
  SELECT
    city1,
    city2,
    avg_distance,
    CASE
      WHEN avg_distance < 1000 THEN '0'
      WHEN avg_distance < 2000 THEN '1000'
      WHEN avg_distance < 3000 THEN '2000'
      WHEN avg_distance < 4000 THEN '3000'
      WHEN avg_distance < 5000 THEN '4000'
      WHEN avg_distance < 6000 THEN '5000'
      ELSE '6000+'
    END AS distance_bin
  FROM avg_pair_distances
),
bin_counts AS (
  SELECT
    distance_bin,
    COUNT(*) AS num_pairs
  FROM binned_pairs
  GROUP BY distance_bin
)
SELECT num_pairs
FROM bin_counts
WHERE num_pairs = (SELECT MIN(num_pairs) FROM bin_counts);
