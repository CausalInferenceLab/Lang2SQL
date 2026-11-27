WITH ordered_locations AS (
    SELECT
        geolocation_city AS city,
        LAG(geolocation_city) OVER city_order AS previous_city,
        RADIANS(geolocation_lat) AS lat_rad,
        RADIANS(geolocation_lng) AS lng_rad,
        LAG(RADIANS(geolocation_lat)) OVER city_order AS previous_lat_rad,
        LAG(RADIANS(geolocation_lng)) OVER city_order AS previous_lng_rad
    FROM olist_geolocation
    WINDOW city_order AS (
        ORDER BY geolocation_state, geolocation_city,
                 geolocation_zip_code_prefix, geolocation_lat, geolocation_lng
    )
), distances AS (
    SELECT previous_city, city,
           6371 * ACOS(MIN(1.0, MAX(-1.0,
               COS(previous_lat_rad) * COS(lat_rad) * COS(lng_rad - previous_lng_rad)
               + SIN(previous_lat_rad) * SIN(lat_rad)
           ))) AS distance_km
    FROM ordered_locations
)
SELECT previous_city, city
FROM distances
WHERE distance_km IS NOT NULL
ORDER BY distance_km DESC, previous_city, city
LIMIT 1;
