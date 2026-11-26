WITH accessible_account_ids AS (
    SELECT DISTINCT account_id
    FROM disp
    WHERE client_id = 3541
),
transaction_totals AS (
    SELECT account_id,
           COUNT(*) AS transaction_count,
           SUM(CASE WHEN type = 'PRIJEM' THEN amount ELSE 0 END) AS total_income,
           SUM(CASE WHEN type = 'VYDAJ' THEN amount ELSE 0 END) AS total_expense
    FROM trans
    WHERE account_id IN (SELECT account_id FROM accessible_account_ids)
    GROUP BY account_id
),
regional_client_counts AS (
    SELECT d.A3 AS region, COUNT(*) AS regional_client_count
    FROM client AS c
    JOIN district AS d ON d.district_id = c.district_id
    GROUP BY d.A3
),
district_populations AS (
    SELECT district_id, A2 AS district_name, A3 AS region,
           A11 AS average_salary, CAST(A4 AS INTEGER) AS inhabitants
    FROM district
),
ranked_districts AS (
    SELECT d.*,
           RANK() OVER (PARTITION BY region ORDER BY inhabitants DESC) AS population_rank,
           ROW_NUMBER() OVER (
               PARTITION BY region ORDER BY inhabitants DESC, district_id ASC
           ) AS regional_district_number
    FROM district_populations AS d
)
SELECT a.account_id,
       a.date AS opened_date,
       a.frequency,
       d.district_name,
       d.region,
       CASE WHEN a.district_id = c.district_id THEN 'Yes' ELSE 'No' END AS is_home_district,
       d.average_salary,
       COALESCE(l.status, 'none') AS loan_repayment_status,
       COALESCE(l.amount, 0) AS loan_amount,
       COALESCE(t.transaction_count, 0) AS transaction_count,
       COALESCE(t.total_income, 0) AS total_income,
       COALESCE(t.total_expense, 0) AS total_expense,
       COALESCE(t.total_income, 0) - COALESCE(t.total_expense, 0) AS net_balance,
       COALESCE(rc.regional_client_count, 0) AS regional_client_count,
       m.district_name AS most_populous_district,
       m.inhabitants AS most_populous_district_inhabitants,
       d.population_rank AS district_population_rank
FROM accessible_account_ids AS ids
JOIN account AS a ON a.account_id = ids.account_id
JOIN client AS c ON c.client_id = 3541
JOIN ranked_districts AS d ON d.district_id = a.district_id
JOIN ranked_districts AS m ON m.region = d.region AND m.regional_district_number = 1
LEFT JOIN loan AS l ON l.account_id = a.account_id
LEFT JOIN transaction_totals AS t ON t.account_id = a.account_id
LEFT JOIN regional_client_counts AS rc ON rc.region = d.region
ORDER BY transaction_count DESC, a.account_id ASC;
