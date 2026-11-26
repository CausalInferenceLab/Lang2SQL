WITH accessible_accounts AS (
    SELECT a.account_id, a.date AS opened_date, a.frequency, a.district_id,
           c.district_id AS home_district_id
    FROM client AS c
    JOIN account AS a ON a.account_id IN (
        SELECT dp.account_id
        FROM disp AS dp
        WHERE dp.client_id = c.client_id
    )
    WHERE c.client_id = 3541
),
transaction_totals AS (
    SELECT a.account_id, COUNT(t.trans_id) AS transaction_count,
           SUM(CASE WHEN t.type = 'PRIJEM' THEN t.amount ELSE 0 END) AS total_income,
           SUM(CASE WHEN t.type = 'VYDAJ' THEN t.amount ELSE 0 END) AS total_expense
    FROM accessible_accounts AS a
    LEFT JOIN trans AS t ON t.account_id = a.account_id
    GROUP BY a.account_id
),
ranked_districts AS (
    SELECT d.district_id, d.A2 AS district_name, d.A3 AS region,
           d.A11 AS average_salary, CAST(d.A4 AS INTEGER) AS inhabitants,
           RANK() OVER (
               PARTITION BY d.A3 ORDER BY CAST(d.A4 AS INTEGER) DESC
           ) AS population_rank
    FROM district AS d
),
regional_clients AS (
    SELECT d.A3 AS region, COUNT(*) AS client_count
    FROM client AS c
    JOIN district AS d ON d.district_id = c.district_id
    GROUP BY d.A3
)
SELECT a.account_id, a.opened_date, a.frequency, d.district_name, d.region,
       CASE WHEN d.district_id = a.home_district_id THEN 'Yes' ELSE 'No' END AS is_home_district,
       d.average_salary, COALESCE(l.status, 'none') AS loan_repayment_status,
       COALESCE(l.amount, 0) AS loan_amount,
       t.transaction_count, t.total_income, t.total_expense,
       t.total_income - t.total_expense AS net_balance,
       COALESCE(rc.client_count, 0) AS regional_client_count,
       top.district_name AS most_populous_district,
       top.inhabitants AS most_populous_district_inhabitants,
       d.population_rank
FROM accessible_accounts AS a
JOIN transaction_totals AS t ON t.account_id = a.account_id
JOIN ranked_districts AS d ON d.district_id = a.district_id
JOIN ranked_districts AS top ON top.region = d.region AND top.population_rank = 1
LEFT JOIN regional_clients AS rc ON rc.region = d.region
LEFT JOIN loan AS l ON l.account_id = a.account_id
ORDER BY t.transaction_count DESC, a.account_id ASC
LIMIT -1
