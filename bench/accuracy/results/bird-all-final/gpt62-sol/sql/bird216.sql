WITH qualifying_districts AS (
    SELECT DISTINCT c.district_id
    FROM client AS c
    JOIN disp AS d ON d.client_id = c.client_id
    WHERE c.gender = 'F'
      AND d.type = 'OWNER'
      AND EXISTS (SELECT 1 FROM card AS ca WHERE ca.disp_id = d.disp_id)
      AND EXISTS (SELECT 1 FROM loan AS l WHERE l.account_id = d.account_id)
), regional_profiles AS (
    SELECT dt.A3 AS region,
           ROUND(AVG(dt.A11) OVER (PARTITION BY dt.A3), 2) AS average_district_salary,
           ROUND(AVG(dt.A13) OVER (PARTITION BY dt.A3), 2) AS average_district_unemployment_1996,
           dt.A2 AS most_populous_district,
           ROW_NUMBER() OVER (
               PARTITION BY dt.A3
               ORDER BY CAST(dt.A4 AS INTEGER) DESC, dt.A2 ASC
           ) AS population_rank
    FROM qualifying_districts AS qd
    JOIN district AS dt ON dt.district_id = qd.district_id
)
SELECT region, average_district_salary, average_district_unemployment_1996, most_populous_district
FROM regional_profiles
WHERE population_rank = 1
ORDER BY region;
