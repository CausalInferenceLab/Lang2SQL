WITH city_ordered AS (
  SELECT
    geolocation_state,
    geolocation_city,
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    LAG(geolocation_lat) OVER (ORDER BY geolocation_state, geolocation_city, geolocation_zip_code_prefix, geolocation_lat, geolocation_lng) AS prev_lat,
    LAG(geolocation_lng) OVER (ORDER BY geolocation_state, geolocation_city, geolocation_zip_code_prefix, geolocation_lat, geolocation_lng) AS prev_lng,
    LAG(geolocation_city) OVER (ORDER BY geolocation_state, geolocation_city, geolocation_zip_code_prefix, geolocation_lat, geolocation_lng) AS prev_city,
    LAG(geolocation_state) OVER (ORDER BY geolocation_state, geolocation_city, geolocation_zip_code_prefix, geolocation_lat, geolocation_lng) AS prev_state,
    LAG(geolocation_zip_code_prefix) OVER (ORDER BY geolocation_state, geolocation_city, geolocation_zip_code_prefix, geolocation_lat, geolocation_lng) AS prev_zip
  FROM olist_geolocation
), distance_calc AS (
  SELECT
    geolocation_state,
    geolocation_city,
    geolocation_zip_code_prefix,
    prev_state,
    prev_city,
    prev_zip,
    geolocation_lat,
    geolocation_lng,
    prev_lat,
    prev_lng,
    RADIANS(geolocation_lat) AS lat2,
    RADIANS(geolocation_lng) AS lon2,
    RADIANS(prev_lat) AS lat1,
    RADIANS(prev_lng) AS lon1
  FROM city_ordered
  WHERE prev_lat IS NOT NULL AND prev_lng IS NOT NULL
), distances AS (
  SELECT
    geolocation_state,
    geolocation_city,
    geolocation_zip_code_prefix,
    prev_state,
    prev_city,
    prev_zip,
    6371 * ACOS(
      COS(lat1) * COS(lat2) * COS(lon2 - lon1) + SIN(lat1) * SIN(lat2)
    ) AS distance_km
  FROM distance_calc
)
SELECT
  prev_state AS state_1,
  prev_city AS city_1,
  prev_zip AS zip_1,
  geolocation_state AS state_2,
  geolocation_city AS city_2,
  geolocation_zip_code_prefix AS zip_2,
  distance_km
FROM distances
ORDER BY distance_km DESC
LIMIT 1;
