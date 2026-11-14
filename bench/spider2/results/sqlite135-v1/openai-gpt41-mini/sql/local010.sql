WITH city_coordinates AS (
  SELECT 
    ap.airport_code,
    CAST(ap.city AS TEXT) AS city,
    CAST(substr(ap.coordinates, 1, instr(ap.coordinates, ',') - 1) AS FLOAT) AS longitude,
    CAST(substr(ap.coordinates, instr(ap.coordinates, ',') + 1) AS FLOAT) AS latitude
  FROM airports_data ap
),
city_pairs AS (
  SELECT 
    CASE WHEN cc1.city < cc2.city THEN cc1.city ELSE cc2.city END AS city1,
    CASE WHEN cc1.city < cc2.city THEN cc2.city ELSE cc1.city END AS city2,
    cc1.latitude AS lat1,
    cc1.longitude AS long1,
    cc2.latitude AS lat2,
    cc2.longitude AS long2
  FROM flights f
  JOIN city_coordinates cc1 ON f.departure_airport = cc1.airport_code
  JOIN city_coordinates cc2 ON f.arrival_airport = cc2.airport_code
),
calculated_distances AS (
  SELECT
    city1,
    city2,
    2 * 6371 * ASIN(
      SQRT(
        POWER(SIN((RADIANS(lat2 - lat1)) / 2), 2) +
        COS(RADIANS(lat1)) * COS(RADIANS(lat2)) * POWER(SIN((RADIANS(long2 - long1)) / 2), 2)
      )
    ) AS distance
  FROM city_pairs
),
avg_distances AS (
  SELECT city1, city2, AVG(distance) AS avg_distance
  FROM calculated_distances
  GROUP BY city1, city2
),
distance_ranges AS (
  SELECT avg_distance,
    CASE 
      WHEN avg_distance < 1000 THEN '0'
      WHEN avg_distance < 2000 THEN '1000'
      WHEN avg_distance < 3000 THEN '2000'
      WHEN avg_distance < 4000 THEN '3000'
      WHEN avg_distance < 5000 THEN '4000'
      WHEN avg_distance < 6000 THEN '5000'
      ELSE '6000+'
    END AS distance_range
  FROM avg_distances
),
count_by_range AS (
  SELECT distance_range, COUNT(*) AS pair_count
  FROM distance_ranges
  GROUP BY distance_range
)
SELECT pair_count
FROM count_by_range
ORDER BY pair_count ASC
LIMIT 1;
