WITH genders(gender, sort_order) AS (
    VALUES ('F', 1), ('M', 2)
), qualifying_accounts AS (
    SELECT account_id
    FROM trans
    GROUP BY account_id
    HAVING AVG(balance) > 1000
), gold_card_accounts AS (
    SELECT DISTINCT card_disp.account_id
    FROM card AS c
    JOIN disp AS card_disp ON card_disp.disp_id = c.disp_id
    WHERE c.type = 'gold'
), qualifying_districts AS (
    SELECT DISTINCT
        cl.gender,
        d.district_id,
        d.A2 AS district_name,
        CAST(d.A4 AS INTEGER) AS inhabitants,
        d.A11 AS salary,
        d.A10 AS urban_ratio
    FROM gold_card_accounts AS gc
    JOIN qualifying_accounts AS qa ON qa.account_id = gc.account_id
    JOIN account AS a ON a.account_id = gc.account_id
    JOIN disp AS owner ON owner.account_id = a.account_id AND owner.type = 'OWNER'
    JOIN client AS cl ON cl.client_id = owner.client_id
    JOIN district AS d ON d.district_id = a.district_id
    WHERE CAST(d.A4 AS INTEGER) > 100000
), ranked_districts AS (
    SELECT
        gender,
        district_id,
        district_name,
        inhabitants,
        RANK() OVER (PARTITION BY gender ORDER BY inhabitants DESC) AS population_rank,
        COUNT(*) OVER (PARTITION BY gender) AS district_count,
        ROUND(AVG(salary) OVER (PARTITION BY gender), 2) AS average_salary,
        ROUND(AVG(urban_ratio) OVER (PARTITION BY gender), 2) AS average_urban_ratio
    FROM qualifying_districts
)
SELECT
    g.gender,
    COALESCE(r.district_count, 0) AS district_count,
    r.district_name,
    r.inhabitants,
    r.average_salary,
    r.average_urban_ratio
FROM genders AS g
LEFT JOIN ranked_districts AS r ON r.gender = g.gender AND r.population_rank = 1
ORDER BY g.sort_order, r.district_name, r.district_id;
