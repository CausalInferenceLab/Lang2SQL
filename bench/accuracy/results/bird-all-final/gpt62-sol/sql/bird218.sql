WITH card_districts AS (
    SELECT DISTINCT
        c.type AS card_type,
        d.district_id,
        d.A2 AS home_district,
        CAST(d.A4 AS INTEGER) AS inhabitants
    FROM card AS c
    JOIN disp AS p ON p.disp_id = c.disp_id
    JOIN client AS cl ON cl.client_id = p.client_id
    JOIN district AS d ON d.district_id = cl.district_id
    WHERE c.type IN ('gold', 'classic', 'junior')
), ranked AS (
    SELECT card_type, home_district, inhabitants,
           DENSE_RANK() OVER (PARTITION BY card_type ORDER BY inhabitants) AS population_rank
    FROM card_districts
)
SELECT card_type, home_district, inhabitants
FROM ranked
WHERE population_rank = 1
ORDER BY card_type, home_district;
