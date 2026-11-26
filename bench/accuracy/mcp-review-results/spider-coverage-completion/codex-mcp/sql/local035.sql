WITH adjacent AS (
    SELECT
        LAG(geolocation_city) OVER location_order AS previous_city,
        geolocation_city AS current_city,
        LAG(RADIANS(geolocation_lat)) OVER location_order AS previous_lat,
        RADIANS(geolocation_lat) AS current_lat,
        LAG(RADIANS(geolocation_lng)) OVER location_order AS previous_lng,
        RADIANS(geolocation_lng) AS current_lng
    FROM main.olist_geolocation
    WINDOW location_order AS (
        ORDER BY geolocation_state, geolocation_city, geolocation_zip_code_prefix,
                 geolocation_lat, geolocation_lng
    )
),
distances AS (
    SELECT
        previous_city,
        current_city,
        6371.0 * ACOS(MAX(-1.0, MIN(1.0,
            COS(previous_lat) * COS(current_lat) * COS(current_lng - previous_lng)
            + SIN(previous_lat) * SIN(current_lat)
        ))) AS distance_km
    FROM adjacent
)
SELECT previous_city, current_city, distance_km
FROM distances
WHERE distance_km = (SELECT MAX(distance_km) FROM distances)
LIMIT 1000
