WITH CityCoords AS (
  SELECT airport_code, 
         CAST(json_extract(city, '$.en') AS TEXT) AS city_en,
         CAST(substr(coordinates, 2, instr(coordinates, ',') - 2) AS REAL) AS latitude,
         CAST(substr(coordinates, instr(coordinates, ',') + 1, length(coordinates) - instr(coordinates, ',') - 1) AS REAL) AS longitude
  FROM airports_data
),
AbakanAirports AS (
  SELECT airport_code 
  FROM CityCoords 
  WHERE city_en = 'Abakan'
),
FlightPairs AS (
  SELECT 
    f.departure_airport, f.arrival_airport,
    cc1.city_en AS city1,
    cc2.city_en AS city2,
    cc1.latitude AS lat1,
    cc1.longitude AS lon1,
    cc2.latitude AS lat2,
    cc2.longitude AS lon2
  FROM flights f
  JOIN CityCoords cc1 ON f.departure_airport = cc1.airport_code
  JOIN CityCoords cc2 ON f.arrival_airport = cc2.airport_code
  WHERE f.departure_airport IN (SELECT airport_code FROM AbakanAirports) 
     OR f.arrival_airport IN (SELECT airport_code FROM AbakanAirports)
),
Distances AS (
  SELECT 
    city1,
    city2,
    2 * 6371 * ASIN(
      SQRT(
        POWER(SIN(RADIANS(lat2 - lat1) / 2), 2) +
        COS(RADIANS(lat1)) * COS(RADIANS(lat2)) *
        POWER(SIN(RADIANS(lon2 - lon1) / 2), 2)
      )
    ) AS distance_km
  FROM FlightPairs
)
SELECT MAX(distance_km) AS max_distance_km FROM Distances;
