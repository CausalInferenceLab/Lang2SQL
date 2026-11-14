WITH ordered_cities AS (
  SELECT
    geolocation_state,
    geolocation_city,
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    ROW_NUMBER() OVER (
      ORDER BY geolocation_state, geolocation_city, geolocation_zip_code_prefix, geolocation_lat, geolocation_lng
    ) AS rn
  FROM
    olist_geolocation
  GROUP BY
    geolocation_state,
    geolocation_city
),
city_pairs AS (
  SELECT
    c1.geolocation_state AS state1,
    c1.geolocation_city AS city1,
    c1.geolocation_lat AS lat1,
    c1.geolocation_lng AS lng1,
    c2.geolocation_state AS state2,
    c2.geolocation_city AS city2,
    c2.geolocation_lat AS lat2,
    c2.geolocation_lng AS lng2,
    6371 * ACOS(
      COS(RADIANS(c1.geolocation_lat)) * COS(RADIANS(c2.geolocation_lat)) *
      COS(RADIANS(c2.geolocation_lng) - RADIANS(c1.geolocation_lng)) +
      SIN(RADIANS(c1.geolocation_lat)) * SIN(RADIANS(c2.geolocation_lat))
    ) AS distance_km
  FROM
    ordered_cities c1
    JOIN ordered_cities c2 ON c2.rn = c1.rn + 1
)
SELECT
  state1, city1, state2, city2, distance_km
FROM
  city_pairs
ORDER BY
  distance_km DESC
LIMIT 1;
