WITH card_districts AS (
    SELECT DISTINCT c.type AS card_type, h.district_id
    FROM main.card AS c
    JOIN main.disp AS p ON p.disp_id = c.disp_id
    JOIN main.client AS h ON h.client_id = p.client_id
    WHERE c.type IN ('gold', 'classic', 'junior')
),
ranked_districts AS (
    SELECT cd.card_type,
           d.A2 AS home_district,
           CAST(d.A4 AS INTEGER) AS inhabitants,
           RANK() OVER (
               PARTITION BY cd.card_type
               ORDER BY CAST(d.A4 AS INTEGER)
           ) AS population_rank
    FROM card_districts AS cd
    JOIN main.district AS d ON d.district_id = cd.district_id
)
SELECT card_type, home_district, inhabitants
FROM ranked_districts
WHERE population_rank = 1
ORDER BY card_type, home_district
LIMIT -1
